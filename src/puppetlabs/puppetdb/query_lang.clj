(ns puppetlabs.puppetdb.query-lang
  (:require [instaparse.core :as insta]
            [puppetlabs.puppetdb.zip :as zip]
            [puppetlabs.puppetdb.query-eng.engine :as eng]
            [clojure.core.match :as cm]))


;; root :query

;; rule(:query)              { compound_statement | nested_statement | statement >> whitespace?}
;; rule(:compound_statement) { (nested_statement | statement) >> whitespace >> join >> whitespace >> query }
;; rule(:nested_statement)   { lparens >> query >> rparens }
;; rule(:statement)          { object_attributes >> whitespace? >> operator >> whitespace? >> literal }
;; rule(:object_attributes)  { object >> (dot >> attribute).repeat(1) }
;; rule(:object)             { ( "nodes" | "classes" | "resources" | "events" ) }

;; rule(:literal)        { boolean | resource | string | float | integer | value }
;; rule(:join)           { ('and' | 'AND') | ('or' | 'OR') }
;; rule(:boolean)        { quote? >> ('true' | 'false') >> quote? }
;; rule(:operator)       { '==' | '!=~' | '!=' | '>=' | '<=' | '>' | '<' | '=~' | '=' | '~' }
;; rule(:identifier)     { quote? >> match('[A-Za-z0-9_\-]').repeat(1) >> quote? }
;; rule(:value)          { quote? >> match('[A-Za-z0-9_\-\*\/\.]').repeat(1) >> quote? }
;; rule(:string)         { quote >> (escape | nonquote).repeat(1) >> quote }
;; rule(:integer)        { (str('+') | str('-')).maybe >> match('[0-9]').repeat(1) }
;; rule(:float)          { integer >> ( dot >> match('[0-9]').repeat(1) | str('e') >> match('[0-9]').repeat(1)) }
;; rule(:resource)       { identifier >> lbracket >> identifier >> rbracket }

;; rule(:whitespace?)    { match('\s+').repeat.maybe }
;; rule(:whitespace)     { match('\s+').repeat }
;; rule(:dot)            { '.' }
;; rule(:comma)          { ',' }
;; rule(:quote)          { "'" | '"' }
;; rule(:quote?)         { quote.maybe }
;; rule(:nonquote)       { "'".absnt? >> any }
;; rule(:escape)         { '\\' >> any }
;; rule(:forward_slash)  { any.maybe >> '/' >> any.maybe }
;; rule(:lbracket)       { '[' >> whitespace? }
;; rule(:rbracket)       { ']' >> whitespace? }
;; rule(:lparens)        { '(' >> whitespace? }
;; rule(:rparens)        { ')' >> whitespace? }

;;    string = { quote (escape | nonquote)+  quote }
;;    nonquote = { "'".absnt? >> any }

;;nodes.trusted.certname =~ web AND classes.name = Apache
;;resources.name = File['/etc/passwd'] AND resources.mode = 0755
;;nodes.name =~ ^web(\d+).*
;;(nodes.puppet_version = 3.5 OR nodes.pe_version = "3.2") AND
;;nodes.is_virtual = true

;;nodes.certname = 'foo'

;;need to fix whitespace and escaping in strings
(def grammar
  (insta/parser
   " <query> = statement | compound-statement
     compound-statement  = statement <whitespace> join <whitespace> query
     statement = object-attributes <whitespace> operator <whitespace> literal
     object-attributes = object <dot> attribute
     object = 'nodes' | 'facts'
     attribute = #'[a-zA-z]+((_|-)[a-zA-z]+)*'
     compound = literal (<whitespace+> join <whitespace+> compound )*
     operator = '=' | '~'
     dot = '.'
    literal = integer | float | string | boolean
    join = ('and' | 'AND') | ('or' | 'OR')
    integer = ('+' | '-') num+
    float = integer ( '.' num | 'e' num )
    string = <'\\''> ( '\\'' | non-quote )+ <'\\''>
    boolean = 'true' | 'false'
    num = #'[0-9]'
    < non-quote > = #'[a-zA-Z]*'
    < whitespace > = #'\\s'

"))

(defn create-subqueries [node]
  (when (and (coll? node)
             (= 3 (count node)))
    (let [ctx (:context (meta node))]
      ["select-nodes"
       node])))

(defn transform-statement [ast state]
  (cm/match [ast]
            [[:statement
              [:object-attributes [:object obj] [:attribute attr]]
              [:operator op]
              [:literal [t literal]]]]
            {:node (with-meta [op attr literal]
                     {:context obj})
             :state (when (empty? state) obj)}

            [[:compound-statement left [:join "and"] right]]
            {:node ["and" left right] :state state}

            :else nil))

(def object->query {"nodes" eng/nodes-query})

(defn convert [ast]
  (let [{:keys [node state]} (zip/pre-order-visit (zip/tree-zipper ast) nil [transform-statement])]
    (eng/compile-user-query->sql (object->query state) #spy/d (first node))))
