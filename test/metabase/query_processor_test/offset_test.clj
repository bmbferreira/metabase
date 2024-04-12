(ns metabase.query-processor-test.offset-test
  "Tests for the new :offset window function clause (#9393)."
  (:require
   [clojure.test :refer :all]
   [java-time.api :as t]
   [malli.core :as mc]
   [malli.error :as me]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.metadata.jvm :as lib.metadata.jvm]
   [metabase.lib.test-metadata :as meta]
   [metabase.query-processor :as qp]
   [metabase.test :as mt]))

(defn- ->local-date [t]
  (t/local-date
   (cond-> t
     (instance? java.time.Instant t)
     (t/zoned-date-time (t/zone-id "UTC")))))

(deftest ^:parallel simple-offset-test
  (mt/test-drivers (mt/normal-drivers-with-feature :window-functions)
    (let [metadata-provider (lib.metadata.jvm/application-database-metadata-provider (mt/id))
          orders            (lib.metadata/table metadata-provider (mt/id :orders))
          orders-created-at (lib.metadata/field metadata-provider (mt/id :orders :created_at))
          orders-total      (lib.metadata/field metadata-provider (mt/id :orders :total))
          query             (-> (lib/query metadata-provider orders)
                                ;; 1. year
                                (lib/breakout (lib/with-temporal-bucket orders-created-at :year))
                                ;; 2. sum(total)
                                (lib/aggregate (lib/sum orders-total))
                                ;; 3. sum(total) last year
                                (lib/aggregate (lib/offset (lib/sum orders-total) -1))
                                (lib/limit 3)
                                (assoc-in [:middleware :format-rows?] false))]
      (mt/with-native-query-testing-context query
        ;;       1               2         3
        (is (= [[#t "2016-01-01"  42156.94 nil]
                [#t "2017-01-01" 205256.40 42156.94]
                [#t "2018-01-01" 510043.47 205256.40]]
               (mt/formatted-rows [->local-date 2.0 2.0]
                 (qp/process-query query))))))))

(deftest ^:parallel offset-aggregation-test
  (testing "yearly growth (this year sales vs last year sales) (#5606)"
    (mt/test-drivers (mt/normal-drivers-with-feature :window-functions)
      (let [metadata-provider (lib.metadata.jvm/application-database-metadata-provider (mt/id))
            orders            (lib.metadata/table metadata-provider (mt/id :orders))
            orders-created-at (lib.metadata/field metadata-provider (mt/id :orders :created_at))
            orders-total      (lib.metadata/field metadata-provider (mt/id :orders :total))
            query             (-> (lib/query metadata-provider orders)
                                  ;; 1. year
                                  (lib/breakout (lib/with-temporal-bucket orders-created-at :year))
                                  ;; 2. sum(total)
                                  (lib/aggregate (lib/sum orders-total))
                                  ;; 3. yearly growth -- sum(total) / offset(sum(total), -1)
                                  (lib/aggregate (lib/- (lib// (lib/sum orders-total)
                                                               (lib/offset (lib/sum orders-total) -1))
                                                        1.0))
                                  (assoc-in [:middleware :format-rows?] false))]
        (mt/with-native-query-testing-context query
          ;;       1               2       3
          (is (= [[#t "2016-01-01"  42156.94 nil]  ; first year
                  [#t "2017-01-01" 205256.40 3.87] ; sales up 387% wow!
                  [#t "2018-01-01" 510043.47 1.48] ; 248% growth!
                  [#t "2019-01-01" 577064.96 0.13] ; 13% growth doesn't look like a hockey stick to me!
                  [#t "2020-01-01" 176095.93 -0.69]] ; sales down by 69%, oops!
                 (mt/formatted-rows [->local-date 2.0 2.0]
                   (qp/process-query query)))))))))

(deftest ^:parallel offset-aggregation-two-breakouts-test
  (mt/test-drivers (mt/normal-drivers-with-feature :window-functions)
    (let [metadata-provider (lib.metadata.jvm/application-database-metadata-provider (mt/id))
          orders            (lib.metadata/table metadata-provider (mt/id :orders))
          orders-created-at (lib.metadata/field metadata-provider (mt/id :orders :created_at))
          orders-total      (lib.metadata/field metadata-provider (mt/id :orders :total))
          query             (-> (lib/query metadata-provider orders)
                                ;; 1. year
                                (lib/breakout (lib/with-temporal-bucket orders-created-at :year))
                                ;; 2. month
                                (lib/breakout (lib/with-temporal-bucket orders-created-at :month))
                                ;; 3. sum(total)
                                (lib/aggregate (lib/sum orders-total))
                                ;; 4. monthly growth %
                                (lib/aggregate (lib/* (lib/- (lib// (lib/sum orders-total)
                                                                    (lib/offset (lib/sum orders-total) -1))
                                                             1.0)
                                                      100.0))
                                (lib/limit 12)
                                (assoc-in [:middleware :format-rows?] false))]
      (mt/with-native-query-testing-context query
        ;;       1               2               3        4
        (is (= [[#t "2016-01-01" #t "2016-04-01" 52.76    nil]
                [#t "2016-01-01" #t "2016-05-01" 1265.73  2299.03]
                [#t "2016-01-01" #t "2016-06-01" 2072.92  63.77]
                [#t "2016-01-01" #t "2016-07-01" 3734.72  80.17]
                [#t "2016-01-01" #t "2016-08-01" 4960.65  32.83]
                [#t "2016-01-01" #t "2016-09-01" 5372.09  8.29]
                [#t "2016-01-01" #t "2016-10-01" 7702.93  43.39]
                [#t "2016-01-01" #t "2016-11-01" 7926.69  2.9]
                [#t "2016-01-01" #t "2016-12-01" 9068.45  14.4]
                [#t "2017-01-01" #t "2017-01-01" 11094.77 nil] ; <- should reset here because breakout 1 changed values
                [#t "2017-01-01" #t "2017-02-01" 11243.66 1.34]
                [#t "2017-01-01" #t "2017-03-01" 14115.68 25.54]]
               (mt/formatted-rows [->local-date ->local-date 2.0 2.0]
                 (qp/process-query query))))))))

(deftest ^:parallel rolling-window-test
  (mt/test-drivers (mt/normal-drivers-with-feature :window-functions)
    (testing "Rolling windows: rolling total of sales last 3 months (#8977)"
      (let [metadata-provider (lib.metadata.jvm/application-database-metadata-provider (mt/id))
            orders            (lib.metadata/table metadata-provider (mt/id :orders))
            orders-created-at (lib.metadata/field metadata-provider (mt/id :orders :created_at))
            orders-total      (lib.metadata/field metadata-provider (mt/id :orders :total))
            query             (-> (lib/query metadata-provider orders)
                                ;; 1. month
                                  (lib/breakout (lib/with-temporal-bucket orders-created-at :month))
                                ;; 2. sum(total)
                                  (lib/aggregate (lib/sum orders-total))
                                ;; 3. rolling total of sales last 3 months
                                  (lib/aggregate (lib/+ (lib/sum orders-total)
                                                        (lib/offset (lib/sum orders-total) -1)
                                                        (lib/offset (lib/sum orders-total) -2)))
                                  (lib/limit 5)
                                  (assoc-in [:middleware :format-rows?] false))]
        (mt/with-native-query-testing-context query
        ;;       1               2        3
          (is (= [[#t "2016-04-01" 52.76   nil]
                  [#t "2016-05-01" 1265.73 nil]
                  [#t "2016-06-01" 2072.92 3391.41]   ; (+ 2072.92 1265.73 52.76)
                  [#t "2016-07-01" 3734.72 7073.37]   ; (+ 3734.72 2072.92 1265.73)
                  [#t "2016-08-01" 4960.65 10768.29]] ; (+ 4960.65 3734.72 2072.92)
                 (mt/formatted-rows [->local-date 2.0 2.0]
                   (qp/process-query query)))))))))

(deftest ^:parallel lead-test
  (mt/test-drivers (mt/normal-drivers-with-feature :window-functions)
    (testing "Rolling windows: sales for current month and next month (LEAD instead of LAG)"
      (let [metadata-provider (lib.metadata.jvm/application-database-metadata-provider (mt/id))
            orders            (lib.metadata/table metadata-provider (mt/id :orders))
            orders-created-at (lib.metadata/field metadata-provider (mt/id :orders :created_at))
            orders-total      (lib.metadata/field metadata-provider (mt/id :orders :total))
            query             (-> (lib/query metadata-provider orders)
                                  ;; 1. month
                                  (lib/breakout (lib/with-temporal-bucket orders-created-at :month))
                                  ;; 2. sum(total)
                                  (lib/aggregate (lib/sum orders-total))
                                  ;; 3. rolling total of sales last 3 months
                                  (lib/aggregate (lib/+ (lib/sum orders-total)
                                                        (lib/offset (lib/sum orders-total) 1)))
                                  (lib/limit 4)
                                  (assoc-in [:middleware :format-rows?] false))]
        (mt/with-native-query-testing-context query
          ;;       1               2        3
          (is (= [[#t "2016-04-01" 52.76   1318.49] ; (+ 52.76 1265.73)
                  [#t "2016-05-01" 1265.73 3338.65] ; (+ 1265.73 2072.92)
                  [#t "2016-06-01" 2072.92 5807.64]
                  [#t "2016-07-01" 3734.72 8695.37]]
                 (mt/formatted-rows [->local-date 2.0 2.0]
                   (qp/process-query query)))))))))


(defn x []
  )

(let [query (-> (lib/query meta/metadata-provider (meta/table-metadata :orders))
                (lib/expression "x" (lib/count)))]
  (or (me/humanize (mc/explain :metabase.lib.schema/query query))
      query))
