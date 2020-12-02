(ns metabase.api.advanced-computation
  "/api/advanced_computation endpoints, like pivot table generation"
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [POST]]
            [metabase.api.common :as api]
            [metabase.models.database :as database :refer [Database]]
            [metabase.query-processor :as qp]
            [metabase.query-processor.pivot :as pivot]
            [metabase.util.i18n :refer [tru]]
            [schema.core :as s]))

(api/defendpoint POST "/pivot/dataset"
  "Generate a pivoted dataset for an ad-hoc query"
  [:as {{:keys      [database]
         query-type :type
         :as        query} :body}]
  {database (s/maybe s/Int)}

  (when-not database
    (throw (Exception. (str (tru "`database` is required for all queries.")))))
  (api/read-check Database database)

  ;; aggregating the results (simulating a SQL union)
  ;; 1. find the combination of all the columns (which is the union of :breakouts :fields :expressions)
  ;; 2. run that largest query first.
  ;; 3. Start returning from the API:
  ;;    {:data {:native_form nil ;; doesn't work for the fact that we're generating multiple queries
  ;;            :insights nil    ;; doesn't work for the fact that we're generating multiple queries
  ;;            :cols <from first query> 
  ;;            :results_metadata <from first query> 
  ;;            :results_timezone <from first query>
  ;;            :rows []         ;; map the :rows responses from *all* queries together, using lazy-cat, adding breakout indicator and additional columns / setting to nil as necessary
  ;;            }
  ;;     :row_count <a total count from the rows collection above
  ;;     :status :completed}
  
  (let [all-queries     (pivot/generate-queries query)
        reverse-compare (fn [a b] (compare b a))
        sorted-queries  (log/spy :error (sort (comp < count :breakout) all-queries))]
    (for [pivot-query (pivot/generate-queries query)]
      (qp/process-query (assoc query :query (:query pivot-query))))
    ))

(api/define-routes)
