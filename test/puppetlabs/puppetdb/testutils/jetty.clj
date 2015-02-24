(ns puppetlabs.puppetdb.testutils.jetty
  (:require [puppetlabs.puppetdb.testutils :refer [temp-dir temp-file]]
            [puppetlabs.puppetdb.fixtures :as fixt]
            [puppetlabs.trapperkeeper.app :as tka]
            [puppetlabs.trapperkeeper.testutils.bootstrap :as tkbs]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-service :refer [jetty9-service]]
            [puppetlabs.trapperkeeper.services.webrouting.webrouting-service :refer [webrouting-service]]
            [puppetlabs.puppetdb.command.constants :refer [command-names]]
            [puppetlabs.puppetdb.client :as pdb-client]
            [puppetlabs.puppetdb.cli.services :refer [puppetdb-service mq-endpoint]]
            [puppetlabs.puppetdb.metrics :refer [metrics-service]]
            [puppetlabs.puppetdb.mq-listener :refer [message-listener-service]]
            [puppetlabs.puppetdb.command :refer [command-service]]
            [puppetlabs.puppetdb.utils :as utils]
            [clj-http.client :as client]
            [puppetlabs.puppetdb.config :as conf]
            [clj-http.util :refer [url-encode]]
            [clj-http.client :as client]
            [clojure.string :as str]
            [fs.core :as fs]
            [slingshot.slingshot :refer [throw+]]))

;; See utils.clj for more information about base-urls.
(def ^:dynamic *base-url* nil) ; Will not have a :version.

(defn log-config
  "Returns a logback.xml string with the specified `log-file` and `log-level`."
  [log-file log-level]
  (str "<configuration>

  <appender name=\"FILE\" class=\"ch.qos.logback.core.FileAppender\">
    <file>" log-file "</file>
    <append>true</append>
    <encoder>
      <pattern>%-4relative [%thread] %-5level %logger{35} - %msg%n</pattern>
    </encoder>
  </appender>

  <root level=\"" log-level "\">
    <appender-ref ref=\"FILE\" />
  </root>
</configuration>"))

(defn create-config
  "Creates a default config, populated with a temporary vardir and
  a fresh hypersql instance"
  []
  {:nrepl {}
   :global {:vardir (temp-dir)}
   :jetty {:port 0}
   :database (fixt/create-db-map)
   :command-processing {}
   :web-router-service {:puppetlabs.puppetdb.cli.services/puppetdb-service ""
                        :puppetlabs.puppetdb.metrics/metrics-service "/metrics"}})

(defn assoc-logging-config
  "Adds a dynamically created logback.xml with a test log. The
  generated log file name is returned for printing to the console."
  [config]
  (let [logback-file (fs/absolute-path (temp-file "logback" ".xml"))
        log-file (fs/absolute-path (temp-file "jett-test" ".log"))]
    (spit logback-file (log-config log-file "ERROR"))
    [log-file (assoc-in config [:global :logging-config] logback-file)]))

(defn current-port
  "Given a trapperkeeper server, return the port of the running jetty instance.
  Note there can be more than one port (i.e. SSL + non-SSL connector). This only
  returns the first one."
  [server]
  (-> @(tka/app-context server)
      (get-in [:WebserverService :jetty9-servers :default :server])
      .getConnectors
      first
      .getLocalPort))

(def ^:dynamic *server*)

(defn puppetdb-instance
  "Stands up a puppetdb instance with `config`, tears down once `f` returns.
  `services` is a seq of additional services that should be started in addition
  to the core PuppetDB services. Binds *server* and *base-url* to refer to
  the instance."
  ([f] (puppetdb-instance (create-config) f))
  ([config f] (puppetdb-instance config [] f))
  ([config services f]
   (let [[log-file config] (-> config conf/adjust-tk-config assoc-logging-config)
         prefix (get-in config
                        [:web-router-service
                         :puppetlabs.puppetdb.cli.services/puppetdb-service])]
     (try
       (tkbs/with-app-with-config server
         (concat [jetty9-service puppetdb-service message-listener-service command-service webrouting-service metrics-service]
                 services)
         config
         (binding [*server* server
                   *base-url* (merge {:protocol "http"
                                      :host "localhost"
                                      :port (current-port server)}
                                     (when prefix {:prefix prefix}))]
           (f)))
       (finally
         (let [log-contents (slurp log-file)]
           (when-not (str/blank? log-contents)
             (utils/println-err "-------Begin PuppetDB Instance Log--------------------\n"
                                log-contents
                                "\n-------End PuppetDB Instance Log----------------------"))))))))

(defmacro with-puppetdb-instance
  "Convenience macro to launch a puppetdb instance"
  [& body]
  `(puppetdb-instance
    (fn []
      ~@body)))

(def max-attempts 50)

(defn command-mbean-name
  "The full mbean name of the MQ destination used for commands"
  [base-url]
  (str "org.apache.activemq:BrokerName="
       (url-encode (:host base-url))
       ",Type=Queue,Destination="
       mq-endpoint))

(defn mq-mbeans-found?
  "Returns true if the ActiveMQ mbeans and the discarded command
  mbeans are found in `mbean-map`"
  [mbean-map]
  (let [mbean-names (map utils/kwd->str (keys mbean-map))]
    (and (some #(.startsWith % "org.apache.activemq") mbean-names)
         (some #(.startsWith % "puppetlabs.puppetdb.command") mbean-names)
         (some #(.startsWith % (command-mbean-name *base-url*)) mbean-names))))

(defn metrics-up?
  "Returns true if the metrics endpoint (and associated jmx beans) are
  up, otherwise will continue to retry. Will fail after trying for
  roughly 5 seconds."
  []
  (let [mbeans-url (str (utils/base-url->str (assoc *base-url* :prefix "/metrics" :version :v1))
                        "/mbeans")]
    (loop [attempts 0]
      (let [{:keys [status body] :as response} (client/get mbeans-url {:as :json
                                                                       :throw-exceptions false})]
        (cond

         (and (= 200 status)
              (mq-mbeans-found? body))
         true

         (<= max-attempts attempts)
         (throw+ response "JMX not up after %s attempts" attempts)

         :else
         (do
           (Thread/sleep 100)
           (recur (inc attempts))))))))

(defn queue-metadata
  "Return command queue metadata (from the `puppetdb-instance` launched PuppetDB) as a map:

  EnqueueCount - the total number of messages sent to the queue since the last restart
  DequeueCount - the total number of messages removed from the queue (ack'd by consumer) since last restart
  InflightCount - the number of messages sent to a consumer session and have not received an ack
  DispatchCount - the total number of messages sent to consumer sessions (Dequeue + Inflight)
  ExpiredCount - the number of messages that were not delivered because they were expired

  http://activemq.apache.org/how-do-i-find-the-size-of-a-queue.html"
  []
  ;; When starting up a `puppetdb-instance` there seems to be a brief
  ;; period of time that the server is up, the broker has been
  ;; started, but the JMX beans have not been initialized, so querying
  ;; for queue metrics fails, this check ensures it's started
  (let [base-metrics-url (assoc *base-url* :prefix "/metrics" :version :v1)]
    (-> (str (utils/base-url->str base-metrics-url)
             "/mbeans/"
             (command-mbean-name base-metrics-url))
        (client/get {:as :json})
        :body)))

(defn current-queue-depth
  "Return the queue depth currently running PuppetDB instance (see `puppetdb-instance` for launching PuppetDB)"
  []
  (:QueueSize (queue-metadata)))

(defn discard-count
  "Return the number of discarded messages from the command queue for the current running `puppetdb-instance`"
  []
  (let [base-metrics-url (assoc *base-url* :prefix "/metrics" :version :v1)]
    (-> (str (utils/base-url->str base-metrics-url)
             "/mbeans/puppetlabs.puppetdb.command:type=global,name=discarded")
        (client/get {:as :json})
        (get-in [:body :Count]))))

(defn until-consumed
  "Invokes `f` and blocks until the `num-messages` have been consumed
  from the commands queue. `num-messages` defaults to 1 if not
  provided. Returns the result of `f` if successful. Requires JMX to
  be enabled in ActiveMQ (the default, but `without-jmx` will cause
  this to fail).

  Exceptions thrown in the following cases:

  timeout - if the message isn't consumed in approximately 5 seconds
  new message in the DLO - if any message is discarded"
  ([f] (until-consumed 1 f))
  ([num-messages f]
   (metrics-up?)
   (let [{start-queue-depth :QueueSize
          start-committed-msgs :DequeueCount
          :as start-queue-metadata} (queue-metadata)
          start-discarded-count (discard-count)
          result (f)
          start-time (System/currentTimeMillis)]

     (loop [{curr-queue-depth :QueueSize
             curr-committed-msgs :DequeueCount
             :as curr-queue-metadata} (queue-metadata)
             curr-discarded-count (discard-count)
             attempts 0]

       (cond

        (< start-discarded-count curr-discarded-count)
        (throw+ {:original-queue-metadata start-queue-metadata
                 :original-discarded-count start-discarded-count
                 :current-queue-metadata curr-queue-metadata
                 :current-discarded-count curr-discarded-count}
                "Found %s new message(s) in the DLO" (- curr-discarded-count start-discarded-count))

        (= attempts max-attempts)
        (let [fail-time (System/currentTimeMillis)]
          (throw+ {:attempts max-attempts
                   :start-time start-time
                   :end-time fail-time}
                  "Failing after %s attempts and %s milliseconds" max-attempts (- fail-time start-time)))

        (or (< 0 curr-queue-depth)
            (< curr-committed-msgs
               (+ start-committed-msgs num-messages)))
        (do
          (Thread/sleep 100)
          (recur (queue-metadata) (discard-count) (inc attempts)))

        :else
        result)))))

(defn dispatch-count
  "Return the queue depth currently running PuppetDB instance (see `puppetdb-instance` for launching PuppetDB)"
  [dest-name]
  (let [base-metrics-url (assoc *base-url* :prefix "/metrics" :version :v1)]
    (-> (str (utils/base-url->str base-metrics-url)
             "/mbeans/org.apache.activemq:BrokerName="
             (url-encode (:host base-metrics-url))
             ",Type=Queue,Destination="
             dest-name)
        (client/get {:as :json})
        (get-in [:body :DispatchCount]))))

(defmacro without-jmx
  "Disable ActiveMQ's usage of JMX. If you start two AMQ brokers in
  the same instance, their JMX beans will collide. Disabling JMX will
  allow them both to be started."
  [& body]
  `(with-redefs [puppetlabs.puppetdb.mq/enable-jmx (fn [broker# _#]
                                                     (.setUseJmx broker# false))]
     (do ~@body)))

(defn sync-command-post [base-url cmd version payload]
  (until-consumed
   (fn []
     (pdb-client/submit-command-via-http! base-url cmd version payload))))
