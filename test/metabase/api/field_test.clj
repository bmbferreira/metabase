(ns metabase.api.field-test
  (:require [expectations :refer :all]
            [metabase.db :as db]
            [metabase.models.database :refer [Database]]
            (metabase.models [field :refer [Field]]
                             [field-values :refer [FieldValues]]
                             [table :refer [Table]])
            [metabase.test.data :refer :all]
            [metabase.test.data.users :refer :all]
            [metabase.test.util :as tu]))

;; Helper Fns

(defn- db-details []
  (tu/match-$ (db)
    {:created_at      $
     :engine          "h2"
     :id              $
     :updated_at      $
     :name            "test-data"
     :is_sample       false
     :is_full_sync    true
     :organization_id nil
     :description     nil
     :features        (mapv name (metabase.driver/features (metabase.driver/engine->driver :h2)))}))


;; ## GET /api/field/:id
(expect
    (tu/match-$ (Field (id :users :name))
      {:description     nil
       :table_id        (id :users)
       :table           (tu/match-$ (Table (id :users))
                          {:description     nil
                           :entity_type     nil
                           :visibility_type nil
                           :db              (db-details)
                           :schema          "PUBLIC"
                           :name            "USERS"
                           :display_name    "Users"
                           :rows            15
                           :updated_at      $
                           :entity_name     nil
                           :active          true
                           :id              (id :users)
                           :db_id           (id)
                           :created_at      $})
       :special_type    "name"
       :name            "NAME"
       :display_name    "Name"
       :updated_at      $
       :active          true
       :id              (id :users :name)
       :field_type      "info"
       :visibility_type "normal"
       :position        0
       :preview_display true
       :created_at      $
       :base_type       "TextField"
       :fk_target_field_id nil
       :parent_id       nil})
    ((user->client :rasta) :get 200 (format "field/%d" (id :users :name))))


;; ## GET /api/field/:id/summary
(expect [["count" 75]      ; why doesn't this come back as a dictionary ?
         ["distincts" 75]]
  ((user->client :rasta) :get 200 (format "field/%d/summary" (id :categories :name))))


;; ## PUT /api/field/:id
;; Check that we can update a Field
;; TODO - this should NOT be modifying a field from our test data, we should create new data to mess with
(tu/expect-eval-actual-first
    (tu/match-$ (let [field (Field (id :venues :latitude))]
               ;; this is sketchy. But return the Field back to its unmodified state so it won't affect other unit tests
               (db/upd Field (id :venues :latitude) :special_type "latitude")
               ;; match against the modified Field
               field)
             {:description     nil
              :table_id        (id :venues)
              :special_type    "fk"
              :name            "LATITUDE"
              :display_name    "Latitude"
              :updated_at      $
              :active          true
              :id              $
              :field_type      "info"
              :visibility_type "normal"
              :position        0
              :preview_display true
              :created_at      $
              :base_type       "FloatField"
              :parent_id       nil
              :fk_target_field_id nil})
  ((user->client :crowberto) :put 200 (format "field/%d" (id :venues :latitude)) {:special_type :fk}))

(expect
  ["Invalid Request."
   nil]
  (tu/with-temp Database [{database-id :id} {:name      "Field Test"
                                             :engine    :yeehaw
                                             :details   {}
                                             :is_sample false}]
    (tu/with-temp Table [{table-id :id} {:name   "Field Test"
                                         :db_id  database-id
                                         :active true}]
      (tu/with-temp Field [{field-id :id} {:table_id    table-id
                                           :name        "Field Test"
                                           :base_type   :TextField
                                           :field_type  :info
                                           :active      true
                                           :preview_display true
                                           :position    1}]
        [((user->client :crowberto) :put 400 (format "field/%d" field-id) {:special_type :timestamp_seconds})
         (db/sel :one :field [Field :special_type] :id field-id)]))))


(defn- field->field-values
  "Fetch the `FieldValues` object that corresponds to a given `Field`."
  [table-kw field-kw]
  (db/sel :one FieldValues :field_id (id table-kw field-kw)))

;; ## GET /api/field/:id/values
;; Should return something useful for a field that has special_type :category
(tu/expect-eval-actual-first
    (tu/match-$ (field->field-values :venues :price)
      {:field_id              (id :venues :price)
       :human_readable_values {}
       :values                [1 2 3 4]
       :updated_at            $
       :created_at            $
       :id                    $})
  (do (db/upd FieldValues (:id (field->field-values :venues :price)) :human_readable_values nil) ; clear out existing human_readable_values in case they're set
      ((user->client :rasta) :get 200 (format "field/%d/values" (id :venues :price)))))

;; Should return nothing for a field whose special_type is *not* :category
(expect
    {:values                {}
     :human_readable_values {}}
  ((user->client :rasta) :get 200 (format "field/%d/values" (id :venues :id))))


;; ## POST /api/field/:id/value_map_update

;; Check that we can set values
(tu/expect-eval-actual-first
    [{:status "success"}
     (tu/match-$ (db/sel :one FieldValues :field_id (id :venues :price))
       {:field_id              (id :venues :price)
        :human_readable_values {:1 "$"
                                :2 "$$"
                                :3 "$$$"
                                :4 "$$$$"}
        :values                [1 2 3 4]
        :updated_at            $
        :created_at            $
        :id                    $})]
  [((user->client :crowberto) :post 200 (format "field/%d/value_map_update" (id :venues :price)) {:values_map {:1 "$"
                                                                                                                    :2 "$$"
                                                                                                                    :3 "$$$"
                                                                                                                    :4 "$$$$"}})
   ((user->client :rasta) :get 200 (format "field/%d/values" (id :venues :price)))])

;; Check that we can unset values
(tu/expect-eval-actual-first
    [{:status "success"}
     (tu/match-$ (db/sel :one FieldValues :field_id (id :venues :price))
       {:field_id              (id :venues :price)
        :human_readable_values {}
        :values                [1 2 3 4]
        :updated_at            $
        :created_at            $
        :id                    $})]
  [(do (db/upd FieldValues (:id (field->field-values :venues :price)) :human_readable_values {:1 "$" ; make sure they're set
                                                                                           :2 "$$"
                                                                                           :3 "$$$"
                                                                                           :4 "$$$$"})
       ((user->client :crowberto) :post 200 (format "field/%d/value_map_update" (id :venues :price))
        {:values_map {}}))
   ((user->client :rasta) :get 200 (format "field/%d/values" (id :venues :price)))])

;; Check that we get an error if we call value_map_update on something that isn't a category
(expect "You can only update the mapped values of a Field whose 'special_type' is 'category'/'city'/'state'/'country' or whose 'base_type' is 'BooleanField'."
  ((user->client :crowberto) :post 400 (format "field/%d/value_map_update" (id :venues :id))
   {:values_map {:1 "$"
                 :2 "$$"
                 :3 "$$$"
                 :4 "$$$$"}}))
