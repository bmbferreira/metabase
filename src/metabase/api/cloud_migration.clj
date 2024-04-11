(ns metabase.api.cloud-migration
  "/api/cloud-migration endpoints.
  Only one migration should be happening at any given time.
  But if something weird happens with concurrency, /cancel will
  cancel all of them. "
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [compojure.core :refer [GET POST PUT]]
   [metabase.api.common :as api]
   [metabase.cmd.dump-to-h2 :as dump-to-h2]
   [metabase.config :as config]
   [metabase.models.cloud-migration :refer [CloudMigration]]
   [metabase.models.setting :refer [defsetting]]
   [metabase.models.setting.cache :as setting.cache]
   [metabase.util.i18n :refer [deferred-tru]]
   [metabase.util.log :as log]
   [methodical.core :as methodical]
   [toucan2.core :as t2]
   [toucan2.pipeline :as t2.pipeline]))

(set! *warn-on-reflection* true)

(def ^:private ^String metabase-store-migration-url
  "https://store-api.metabase.com/api/v2/migration")

(def ^:private h2-file
  (.getAbsolutePath (io/file "cloud-migration.db")))


;; Read-Only mode

(defsetting read-only-mode
  (deferred-tru
    (str "Boolean indicating whether a Metabase's is in read-only mode with regards to its app db. "
         "Will take up to 1m to propagate to other Metabase instances in a cluster."
         "Audit tables are excluded from read-only-mode mode."))
  :type       :boolean
  :visibility :admin
  :default    false
  :doc        false
  :export?    false)

(def ^:private read-only-mode-exceptions
  #{;; Migrations need to update their own state
    :model/CloudMigration
    :model/Setting

    ;; Users need to login, make queries, and we need need to audit them.
    :model/Session
    :model/User
    :model/LoginHistory
    :model/AuditLog
    :model/QueryExecution
    :model/ViewLog

    ;; TODO: Came up in tests, check with core folks
    :setting
    :permissions_group_membership})

;; Block write calls to most tables in read-only mode.
(methodical/defmethod t2.pipeline/build :before [#_query-type     :toucan.statement-type/DML
                                                 #_model          :default
                                                 #_resolved-query :default]
  [_query-type model _parsed-args resolved-query]
  (when (and (read-only-mode)
             (not (read-only-mode-exceptions model)))
    (throw (ex-info "Metabase is in read-only-mode mode!" {})))
  resolved-query)


;; Helpers

(def ^:private terminal-states
  #{:done :error :cancelled})

(defn- cluster?
  []
  (>= (t2/count 'QRTZ_SCHEDULER_STATE) 2))

(defn- set-progress
  [id state progress]
  (when (= 0 (t2/update! CloudMigration :id id :state [:not-in terminal-states]
                         {:state state :progress progress}))
    (throw (ex-info "Cannot update migration in terminal state" {:terminal true}))))

(defn- migrate! [{:keys [id]}]
  (try
    ;; Set read-only-mode
    (set-progress id :setup 1)
    (read-only-mode! true)
    (when (cluster?)
      ;; Wait for read-only-mode to propagate to other cluster instances.
      (Thread/sleep (int (* 1.5 setting.cache/cache-update-check-interval-ms))))

    ;; Start dumping the h2 backup
    (set-progress id :dump 20)
    (dump-to-h2/dump-to-h2! h2-file)
    (read-only-mode! false)

    ;; Upload dump to store
    (set-progress id :upload 50)
    ;; TODO: upload
    ;; TODO: track upload progress

    ;; All done
    (set-progress id :done 100)
    (catch Exception e
      (when-not (-> e ex-data :terminal)
        (t2/update! :id id :state :error)
        (log/error e "Error performing migration")))
    (finally
      (read-only-mode! false)
      (io/delete-file h2-file :silently))))

(defn- get-store-migration
  "Calls Store and returns {:external_id ,,, :upload-url ,,,}."
  []
  (-> metabase-store-migration-url
      (http/post {:form-params  {:local_mb_version (config/mb-version-info :tag)}
                  :content-type :json})
      :body
      (json/parse-string keyword)
      (select-keys [:id :upload_url])
      (set/rename-keys {:id :external_id})))


;; Endpoints

(api/defendpoint POST "/"
  "Initiate a new cloud migration."
  [_]
  (api/check-superuser)
  (if (t2/select-one CloudMigration :state [:not-in terminal-states])
    {:status 409 :body "There's an ongoing migration already."}
    (try
      (let [cloud-migration (t2/insert-returning-instance! CloudMigration (get-store-migration))]
        (future (migrate! cloud-migration))
        cloud-migration)
      (catch Exception e
        (condp = (-> e ex-data :status)
          404 {:status 404 :body "Could not establish a connection to Metabase Cloud."}
          400 {:status 400 :body "Cannot migrate this Metabase version."}
          {:status 500})))))

(api/defendpoint GET "/"
  "Get the latest cloud migration, if any."
  [_]
  (api/check-superuser)
  (t2/select-one CloudMigration :state [:not-in terminal-states] {:order-by [[:created_at :desc]]}))

(api/defendpoint PUT "/cancel"
  "Cancel any ongoing cloud migrations, if any."
  [_]
  (api/check-superuser)
  (read-only-mode! false)
  (t2/update! CloudMigration {:state [:not-in terminal-states]} {:state :cancelled}))

(api/define-routes)
