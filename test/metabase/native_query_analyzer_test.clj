(ns metabase.native-query-analyzer-test
  (:require
   [clojure.test :refer :all]
   [metabase.native-query-analyzer :as query-analyzer]
   [metabase.public-settings :as public-settings]
   [metabase.test :as mt]))

(deftest active-test
  (mt/discard-setting-changes [sql-parsing-disabled]
    (testing "sql parsing enabled"
      (public-settings/sql-parsing-disabled! false)
      (binding [query-analyzer/*parse-queries-in-test?* true]
        (is (true? (#'query-analyzer/active?)))))
    (testing "sql parsing disabled"
      (public-settings/sql-parsing-disabled! true)
      (binding [query-analyzer/*parse-queries-in-test?* true]
        (is (false? (#'query-analyzer/active?)))))))
