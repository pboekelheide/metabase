(ns metabase.query-processor.pivot
  "Pivot table actions for the query processor"
  (:require [clojure.tools.logging :as log]))

(defn powerset
  "Generate the set of all subsets"
  [items]
  (reduce (fn [s x]
            (clojure.set/union s (map #(conj % x) s)))
          (hash-set #{})
          items))

(defn- generate-breakouts
  "Generate the combinatorial breakouts for a given query pivot table query"
  [breakouts]
  (powerset (set breakouts)))

(defn generate-queries
  "Generate the additional queries to perform a generic pivot table"
  [request]
  (let [query     (:query request)
        breakouts (generate-breakouts (:breakout query))]
    (map (fn [breakout]
           (-> request
               (assoc-in [:query :breakout] (vec breakout))
               ;;TODO: `pivot-grouping` is not "magic" enough to mark it as an internal thing
               (assoc-in [:query :fields] [[:expression "pivot-grouping"]])
               ;;TODO: replace this value with a bitmask or something to indicate the source better
               (assoc-in [:query :expressions] {"pivot-grouping" [:ltrim (str (vec breakout))]}))) 
         breakouts)))