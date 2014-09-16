(ns com.puppetlabs.puppetdb.query-lang
  (:require [instaparse.core :as insta]) )


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

#_(def grammar
  (insta/parser
   "join = { ('and' | 'AND') | ('or' | 'OR') }
    literal = { boolean | resource | string | float | integer | value }
    resource = { identifier >> lbracket >> identifier >> rbracket }
    integer = { (str('+') | str('-')).maybe >> match('[0-9]').repeat(1) }
    float = { integer >> ( dot >> match('[0-9]').repeat(1) | str('e') >> match('[0-9]').repeat(1)) }
    "))

;;    float = num (  '.'? num | 'e' num )
;;     num = #'[0-9]'
;; rule(:string)         { quote >> (escape | nonquote).repeat(1) >>
;;     quote }


;;need to fix whitespace and escaping in strings
(def grammar3
  (insta/parser
   "compound = literal (<whitespace+> join <whitespace+> compound )*

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



#_(def grammar2
  (insta/parser
   "query = ( compound-statement | nested-statement | statement )+ whitespace*
    compound-statement = ( nested-statement | statement ) whitespace+ join whitespace+ query
    nested-statement = (
    whitespace = #'\\s'

"))
