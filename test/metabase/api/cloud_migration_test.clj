(ns ^:synchronized metabase.api.cloud-migration-test
  "Tests for /api/cloud-migration.
  All tests in this ns should have ^:synchronized since they can toggle read-only-mode."
  (:require
   [clojure.test :refer :all]
   [metabase.api.cloud-migration :as cloud-migration]
   [metabase.models.cloud-migration :refer [CloudMigration]]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(use-fixtures :each (fn [thunk]
                      (mt/discard-setting-changes [read-only-mode]
                          (thunk))))

(defmacro mock-external-calls!
  "Mock external calls around migration creation."
  [& body]
  `(with-redefs [cloud-migration/get-store-migration (constantly {:external_id 1 :upload_url ""})
                 cloud-migration/migrate! identity]
     ~@body))

(deftest ^:synchronized permissions-test
  (testing "Requires superuser"
    (mt/user-http-request :rasta :post 403 "cloud-migration")
    (mt/user-http-request :rasta :get 403 "cloud-migration")
    (mt/user-http-request :rasta :put 403 "cloud-migration/cancel")

    (mock-external-calls! (mt/user-http-request :crowberto :post 200 "cloud-migration"))
    (mt/user-http-request :crowberto :get 200 "cloud-migration")
    (mt/user-http-request :crowberto :put 200 "cloud-migration/cancel")))

(deftest ^:synchronized migrate!-test
  ;; TODO
  )

(deftest ^:synchronized concurrency-test
  ;; The Gods of Concurrency with terror and slaughter return
  (run! (partial t2/insert-returning-instance! CloudMigration)
        [{:external_id 1 :upload_url "" :state :dump}
         {:external_id 2 :upload_url "" :state :cancelled}
         {:external_id 3 :upload_url "" :state :error}
         {:external_id 5 :upload_url "" :state :done}
         {:external_id 4 :upload_url "" :state :setup}])
  (cloud-migration/read-only-mode! true)

  (is (= "setup" (:state (mt/user-http-request :crowberto :get 200 "cloud-migration"))))
  (mt/user-http-request :crowberto :put 200 "cloud-migration/cancel")
  (mt/user-http-request :crowberto :get 204 "cloud-migration")
  (is (not (cloud-migration/read-only-mode))))
