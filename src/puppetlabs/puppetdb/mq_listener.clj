(ns puppetlabs.puppetdb.mq-listener
  (:import [javax.jms ExceptionListener JMSException MessageListener Session])
  (:require [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.command.dlo :as dlo]
            [puppetlabs.puppetdb.mq :as mq]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.cheshire :as json]
            [slingshot.slingshot :refer [try+]]
            [puppetlabs.puppetdb.metrics.core :as metrics]
            [metrics.meters :refer [meter mark!]]
            [metrics.histograms :refer [histogram update!]]
            [metrics.timers :refer [timer time!]]
            [puppetlabs.puppetdb.command :as cmd]
            [puppetlabs.trapperkeeper.services :refer [defservice service-context service-id]]
            [schema.core :as s]
            [puppetlabs.puppetdb.config :as conf]
            [puppetlabs.puppetdb.schema :as pls]
            [clojure.core.async :as async]
            [com.climate.claypoole :as cp]))

;; ## Performance counters

;; For each command (and globally), add command-processing
;; metrics. The hierarchy of command-processing metrics has the
;; following form:
;;
;;     {"global"
;;        {:seen <meter>
;;         :processed <meter>
;;         :fatal <meter>
;;         :retried <meter>
;;         :discarded <meter>
;;         :processing-time <timer>
;;         :retry-counts <histogram>
;;        }
;;      "command name"
;;        {<version number>
;;           {:seen <meter>
;;            :processed <meter>
;;            :fatal <meter>
;;            :retried <meter>
;;            :discarded <meter>
;;            :processing-time <timer>
;;            :retry-counts <histogram>
;;           }
;;        }
;;     }
;;
;; The `"global"` hierarchy contains metrics aggregated across all
;; commands.
;;
;; * `:seen`: the number of commands (valid or not) encountered
;;
;; * `:processeed`: the number of commands successfully processed
;;
;; * `:fatal`: the number of fatal errors
;;
;; * `:retried`: the number of commands re-queued for retry
;;
;; * `:discarded`: the number of commands discarded for exceeding the
;;   maximum allowable retry count
;;
;; * `:processing-time`: how long it takes to process a command
;;
;; * `:retry-counts`: histogram containing the number of times
;;   messages have been retried prior to suceeding
;;
(def mq-metrics-registry (get-in metrics/metrics-registries [:mq :registry]))

(defn create-metrics [prefix]
  (let [to-metric-name-fn (fn [metric-name]
                            (->> metric-name
                                 (conj prefix)
                                 (mapv name)))]
    {:processing-time (timer mq-metrics-registry (to-metric-name-fn :processing-time))
     :retry-counts (histogram mq-metrics-registry (to-metric-name-fn :retry-counts))
     :seen (meter mq-metrics-registry (to-metric-name-fn :seen))
     :processed (meter mq-metrics-registry (to-metric-name-fn :processed))
     :fatal (meter mq-metrics-registry (to-metric-name-fn :fatal))
     :retried (meter mq-metrics-registry (to-metric-name-fn :retried))
     :discarded (meter mq-metrics-registry (to-metric-name-fn :discarded))}))

(def metrics (atom {:global (create-metrics [:global])}))

(defn global-metric
  "Returns the metric identified by `name` in the `\"global\"` metric
  hierarchy"
  [name]
  {:pre [(keyword? name)]}
  (get-in @metrics [:global name]))

(defn cmd-metric
  [cmd version name]
  {:pre [(keyword? name)]}
  (get-in @metrics [(keyword (str cmd version)) name]))

(defn create-metrics-for-command!
  "Create a subtree of metrics for the given command and version (if
  present).  If a subtree of metrics already exists, this function is
  a no-op."
  [command version]
  (let [storage-path [(keyword (str command version))]]
    (when (= ::not-found (get-in @metrics storage-path ::not-found))
      (swap! metrics assoc-in storage-path
             (create-metrics [(keyword command) (keyword (str version))])))))

(defn fatal?
  "Tests if the supplied exception is a fatal command-processing
  exception or not."
  [exception]
  (:fatal exception))

;; ## MQ I/O
;;
;; The data flow through the code is as follows:
;;
;; * A message is read off of an MQ endpoint
;;
;; * The message is fed through a _message processor_: a function that
;;   takes a message as the only argument
;;
;; * Repeat ad-infinitum
;;

;; ## MQ processing middleware
;;
;; The parsing and processing of incoming commands is architected as a
;; set of _middleware_ functions. That is, higher-order functions that
;; add capabilities to an existing message-handling
;; function. Middleware can be stacked, creating sophisticated
;; hierarchies of functionality. And because each middleware function
;; is isolated in terms of capability, testability is much simpler.
;;
;; It's not an original idea; it was stolen from _Ring's_ middleware
;; architecture.
;;

(defn wrap-with-meter
  "Wraps a message processor `f` and a `meter` such that `meter` will be marked
  for each invocation of `f`."
  [f meter]
  (fn [msg]
    (mark! meter)
    (f msg)))

(defn try-parse-command
  "Tries to parse `msg`, returning the parsed message if the parse is
  successful or a Throwable object if one is thrown."
  [msg]
  (try+
   (cmd/parse-command msg)
   (catch Throwable e
     e)))

(defn annotate-with-attempt
  "Adds an `attempt` annotation to `msg` indicating there was a failed attempt
  at handling the message, including the error and trace from `e`."
  [{:keys [annotations] :as msg} e]
  {:pre  [(map? annotations)]
   :post [(= (count (get-in % [:annotations :attempts]))
             (inc (count (:attempts annotations))))]}
  (let [attempts (get annotations :attempts [])
        attempt  {:timestamp (kitchensink/timestamp)
                  :error     (str e)
                  :trace     (map str (.getStackTrace e))}]
    (update-in msg [:annotations :attempts] conj attempt)))

(defn wrap-with-exception-handling
  "Wrap a message processor `f` such that all Throwable or `fatal?`
  exceptions are caught.

  If a `fatal?` exception is thrown, the supplied `on-fatal` function
  is invoked with the message and the exception as arguments.

  If any other Throwable exception is caught, the supplied `on-retry`
  function is invoked with the message and the exception as
  arguments."
  [f on-retry on-fatal]
  (fn [msg]
    (try+

     (mark! (global-metric :seen))
     (time! (global-metric :processing-time)
            (f msg))
     (mark! (global-metric :processed))

     (catch fatal? {:keys [cause]}
       (on-fatal msg cause)
       (mark! (global-metric :fatal)))

     (catch Throwable exception
       (on-retry msg exception)
       (mark! (global-metric :retried))))))

(defn wrap-with-command-parser
  "Wrap a message processor `f` such that all messages passed to `f` are
  well-formed commands. If a message cannot be parsed, the `on-fatal` hook is
  invoked, and `f` is ignored."
  [f on-parse-error]
  (fn [msg]
    (let [parse-result (try-parse-command msg)]
      (if (instance? Throwable parse-result)
        (do
          (mark! (global-metric :fatal))
          (on-parse-error msg parse-result))
        (f parse-result)))))

(defn wrap-with-discard
  "Wrap a message processor `f` such that incoming commands with a
  retry count exceeding `max-retries` are discarded.

  This assumes that all incoming messages are well-formed command
  objects, such as those produced by the `wrap-with-command-parser`
  middleware."
  [f on-discard max-retries]
  (fn [{:keys [command version annotations] :as msg}]
    (let [retries (count (:attempts annotations))]
      (create-metrics-for-command! command version)
      (mark! (cmd-metric command version :seen))
      (update! (global-metric :retry-counts) retries)
      (update! (cmd-metric command version :retry-counts) retries)

      (when (>= retries max-retries)
        (mark! (global-metric :discarded))
        (mark! (cmd-metric command version :discarded))
        (on-discard msg))

      (when (< retries max-retries)
        (let [result (time! (cmd-metric command version :processing-time)
                            (f msg))]
          (mark! (cmd-metric command version :processed))
          result)))))

(defn wrap-with-thread-name
  "Wrap a message processor `f` such that the calling thread's name is
  set to `<prefix>-<thread-id>`. This is useful for situations where
  thread names are ordinarily duplicated across multiple threads, or
  if the default names aren't descriptive enough."
  [f prefix]
  (fn [msg]
    (let [t    (Thread/currentThread)
          name (format "%s-%d" prefix (.getId t))]
      (when-not (= (.getName t) name)
        (.setName t name))
      (f msg))))

(defn handle-command-discard
  [{:keys [command annotations] :as msg} discard]
  (let [attempts (count (:attempts annotations))
        id       (:id annotations)]
    (log/errorf "[%s] [%s] Exceeded max %d attempts" id command attempts)
    (discard msg nil)))

(defn handle-parse-error
  [msg e discard]
  (log/errorf e "Fatal error parsing command: %s" msg)
  (discard msg e))

(defn handle-command-failure
  "Dump the error encountered during command-handling to the log and discard
  the message."
  [{:keys [command annotations] :as msg} e discard]
  (let [attempt (count (:attempts annotations))
        id      (:id annotations)
        msg     (annotate-with-attempt msg e)]
    (log/errorf e "[%s] [%s] Fatal error on attempt %d" id command attempt)
    (discard msg e)))

;; The number of times a message can be retried before we discard it
(def maximum-allowable-retries 16)

(defn handle-command-retry
  "Dump the error encountered to the log, and re-publish the message,
  with an incremented retry counter, after a delay.

  The error is logged to DEBUG level during the first `M/4` retries,
  and ERROR thereafter, where `M` is the maximum number of allowable
  retries.

  The retry delay is based on the following exponential backoff
  algorithm:

    2^(n-1) + random(2^n)

  `n` is the number of the current attempt, and `random` is a random
  number between 0 and the argument."
  [{:keys [command version annotations] :as msg} e publish-fn]
  (mark! (cmd-metric command version :retried))
  (let [attempt (count (:attempts annotations))
        id      (:id annotations)
        msg     (annotate-with-attempt msg e)
        n       (inc attempt)
        delay   (+ (Math/pow 2 (dec n))
                   (rand-int (Math/pow 2 n)))
        error-msg (format "[%s] [%s] Retrying after attempt %d, due to: %s"
                          id command attempt e)]
    (if (> n (/ maximum-allowable-retries 4))
      (log/error e error-msg)
      (log/debug e error-msg))

    (publish-fn (json/generate-string msg) (mq/delay-property delay :seconds))))

(defn wrap-message-handler-middleware
  [send-delayed-msg-fn discarded-dir message-fn]
  (let [discard        #(dlo/store-failed-message %1 %2 discarded-dir)
        on-discard     #(handle-command-discard % discard)
        on-parse-error #(handle-parse-error %1 %2 discard)
        on-fatal       #(handle-command-failure %1 %2 discard)
        on-retry       #(handle-command-retry %1 %2 send-delayed-msg-fn)]
    (-> message-fn
        (wrap-with-discard on-discard maximum-allowable-retries)
        (wrap-with-exception-handling on-retry on-fatal)
        (wrap-with-command-parser on-parse-error)
        (wrap-with-meter (global-metric :seen))
        (wrap-with-thread-name "command-proc"))))

(defprotocol MessageListenerService
  (register-listener [this schema listener-fn])
  (process-message [this message]))

(def message-fn-schema
  (s/make-fn-schema s/Any {s/Any s/Any}))

(def handler-schema
  [[(s/one message-fn-schema "Predicate Function")
    (s/one message-fn-schema "Message Handler Function")]])

(pls/defn-validated matching-handler
  "Takes a list of pred/handler pairs and returns the first matching handler
   for the given message"
  [handlers :- handler-schema
   message :- {s/Any s/Any}]
  (first
   (for [[pred handler] handlers
         :when (pred message)]
     handler)))

(pls/defn-validated conj-handler
  "Conjs the predicate and message handler onto the `listener-atom` list"
  [listener-atom :- clojure.lang.Atom
   pred :- message-fn-schema
   handler-fn :- message-fn-schema]
  (swap! listener-atom conj [pred handler-fn]))

(defn ^javax.jms.Session create-session [^javax.jms.Connection connection]
  (.createSession connection false javax.jms.Session/CLIENT_ACKNOWLEDGE))

(defn ^javax.jms.Queue create-queue [^javax.jms.Session session endpoint]
  (.createQueue session endpoint))

(defn ^javax.jms.Connection create-connection [^javax.jms.ConnectionFactory factory]
  (doto (.createConnection factory)
    (.setExceptionListener
     (reify ExceptionListener
       (onException [this ex]
         (log/error ex "receiver queue connection error"))))
    (.start)))

(defn send-delayed-message [{:keys [^javax.jms.ConnectionFactory conn-pool endpoint]}]
  (fn [message-to-convert message-properties]
    (with-open [connection (create-connection conn-pool)
                session (create-session connection)]
      
      (let [q (create-queue session endpoint)]
        (with-open [producer (.createProducer session q)]

          (.send producer (mq/to-jms-message session message-to-convert message-properties)))))))

(defn create-command-consumer [{:keys [discard-dir] :as mq-context} command-handler]
  (let [message-handler (wrap-message-handler-middleware (send-delayed-message mq-context)
                                                         discard-dir
                                                         command-handler)]
    (fn [^javax.jms.Message message]
      ;; When the queue is shutting down, it sends nil message
      (when message
        (try
          (message-handler (mq/convert-jms-message message))
          (.acknowledge message)
          (catch Throwable ex
            (log/error ex "message receive failed")
            (throw ex)))))))

(defn create-mq-receiver
  [{:keys [^javax.jms.ConnectionFactory conn-pool endpoint]} mq-message-handler]
  (with-open [connection (create-connection conn-pool)
              session (create-session connection)
              consumer (.createConsumer session (create-queue session endpoint))]
    (try
      (loop []
        (mq-message-handler (.receive consumer))
        (recur))

      (catch javax.jms.IllegalStateException e

        (log/trace e "Received IllegalStateException, shutting down")
        (log/info "Received IllegalStateException, shutting down")))))

(defn create-command-handler-threadpool [size]
  {:semaphore (java.util.concurrent.Semaphore. size)
   :threadpool (java.util.concurrent.ThreadPoolExecutor. 1
                                                         Integer/MAX_VALUE
                                                         1
                                                         java.util.concurrent.TimeUnit/MINUTES
                                                         (java.util.concurrent.SynchronousQueue.))})

(defn command-submission-handler
  [{:keys [^java.util.concurrent.Semaphore semaphore
           ^java.util.concurrent.ExecutorService threadpool]}
   handler-fn]
  (fn [message]
    (.acquire semaphore)
    (try
      (.execute threadpool (fn []
                             (try
                               (handler-fn message)
                               (finally
                                 (.release semaphore)))))
      (catch java.util.concurrent.RejectedExecutionException e
        (.release semaphore)))))

(defn run-mq-listener-thread [mq-context message-consumer-fn]
  (doto (Thread. (fn []
                   (create-mq-receiver mq-context message-consumer-fn)))
    (.setDaemon false)
    (.start)))

(defn create-mq-context [config]
  {:discard-dir (conf/mq-discard-dir config)
   :endpoint (conf/mq-endpoint config)
   :conn-pool (mq/activemq-connection-factory (conf/mq-broker-url config))})

(defservice message-listener-service
  MessageListenerService
  [[:DefaultedConfig get-config]
   [:PuppetDBServer]] ; MessageListenerService depends on the broker

  (init [this context]
        (assoc context :listeners (atom [])))

  (start [this context]
    (let [config (get-config)
          mq-context (create-mq-context config)

          command-threadpool (create-command-handler-threadpool (conf/mq-thread-count config))
          
          command-handler #(process-message this %)
          message-handler (create-command-consumer mq-context command-handler)
          mq-message-handler (command-submission-handler command-threadpool message-handler)]

      (run-mq-listener-thread mq-context mq-message-handler)

      (assoc context
             :conn-pool (:conn-pool mq-context)
             :consumer-threadpool command-threadpool)))

  (stop [this {:keys [conn-pool consumer-threadpool receiver-thread] :as context}]
        (.drainPermits (:semaphore consumer-threadpool))
        (.shutdown (:threadpool consumer-threadpool))
        (.close conn-pool)
    context)

  (register-listener [this pred listener-fn]
    (conj-handler (:listeners (service-context this)) pred listener-fn))

  (process-message [this message]
    (if-let [handler-fn (matching-handler @(:listeners (service-context this))
                                          message)]
      (handler-fn message)
      (log/warnf "No message handler found for %s" message))))
