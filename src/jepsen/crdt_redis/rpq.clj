(ns jepsen.crdt-redis.rpq
  (:require [clojure.tools.logging :refer :all]
            [clojure.data.priority-map :refer [priority-map-by]]
            [jepsen.crdt-redis.support :as spt]
            [jepsen [client :as client]
                    [generator :as gen]]
            [knossos.model :as model]
            [taoensso.carmine :as car])
  (:use [slingshot.slingshot :only [try+]])
  (:import [history Invocation]
           [knossos.model Model]))

;; pq
(defn zadd   [_ _] {:type :invoke, :f :add, :value [(rand-int 5) (rand-int 100)]})

(defn zincrby   [_ _] {:type :invoke, :f :incrby, :value [(rand-int 5) (- (rand-int 200) 100)]})

(defn zrem   [_ _] {:type :invoke, :f :rem, :value [(rand-int 5)]})

(defn zscore   [_ _] {:type :invoke, :f :score, :value [(rand-int 5)]})

(defn zmax   [_ _] {:type :invoke, :f :max, :value []})

(defn workload []
  (gen/mix [zadd zincrby zrem zscore zmax]))

(defrecord PQClient [conn type]
  client/Client
  (open! [this test node]
    (assoc this :conn node))

  (setup! [this test])

  (invoke! [_ test op]
    ;; (info conn)
    (case (:f op)
        :add (try+ (do (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "zadd") "default"] (:value op))))
                 (assoc op :type :ok, :value nil))
              (catch [] ex
                  (assoc op :type :fail, :value nil)))
        :incrby (try+ (do (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "zincrby") "default"] (:value op))))
                    (assoc op :type :ok, :value nil))
                (catch [] ex
                  (assoc op :type :fail, :value nil)))
        :rem (try+ (do (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "zrem") "default"] (:value op))))
                 (assoc op :type :ok, :value nil))
              (catch [] ex
                  (assoc op :type :fail, :value nil)))
        :score (assoc op :type :ok, :value (spt/parse-number (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "zscore") "default"] (:value op))))))
        :max (assoc op :type :ok, :value (vec (map spt/parse-number (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "zmax") "default"] (:value op)))))))))

  (teardown! [this test])

  (close! [_ test]))


(defrecord CrdtPQ [pq]
  Model
  (step [this invocation]
    (condp = (.getMethodName invocation)
      "add" (CrdtPQ. (assoc pq (int (spt/get-arg invocation 0)) (int (spt/get-arg invocation 1))))
      "incrby" (CrdtPQ. (assoc pq (int (spt/get-arg invocation 0)) (+ (int (spt/get-arg invocation 1)) (get pq (int (spt/get-arg invocation 1)) 0))))
      "rem" (CrdtPQ. (dissoc pq (int (spt/get-arg invocation 0))))
      "score" (if (if (= (.size (.getRetValues invocation)) 0) (not (contains? pq (int (spt/get-arg invocation 0)))) (= (get pq (int (spt/get-arg invocation 0))) (int (spt/get-ret invocation 0))))
              this
              (model/inconsistent (str "score fails" )))
      "max" (if (if (= (.size (.getRetValues invocation)) 0) (empty? pq) (and (not (empty? pq)) (= (int (get (first pq) 1)) (int (spt/get-ret invocation 1)))))
            this
            (model/inconsistent (str "max fails" ))))))

(defn crdtpq
  []
  (CrdtPQ. (priority-map-by >)))
