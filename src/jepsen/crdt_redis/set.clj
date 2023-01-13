(ns jepsen.crdt-redis.set
  (:require [clojure.tools.logging :refer :all]
            [jepsen.crdt-redis.support :refer [get-arg get-ret]]
            [jepsen [client :as client]
                    [generator :as gen]]
            [knossos.model :as model]
            [taoensso.carmine :as car])
  (:use [slingshot.slingshot :only [try+]])
  (:import [history Invocation]
           [knossos.model Model]))

;; set
(defn sadd   [_ _] {:type :invoke, :f :add, :value [(rand-int 5)]})
(defn sremove   [_ _] {:type :invoke, :f :remove, :value [(rand-int 5)]})
(defn scontains   [_ _] {:type :invoke, :f :contains, :value [(rand-int 5)]})
(defn ssize   [_ _] {:type :invoke, :f :size, :value []})

(defn workload []
  (gen/mix [sadd sremove scontains ssize]))

(defrecord SetClient [conn type]
  client/Client
  (open! [this test node]
    (assoc this :conn node))

  (setup! [this test])

  (invoke! [_ test op]
    ;; (info conn)
    (case (:f op)
      :add (try+ (do (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "sadd") "default"] (:value op))))
                     (assoc op :type :ok, :value nil))
                 (catch [] ex
                   (assoc op :type :fail, :value nil)))
      :remove (try+ (do (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "srem") "default"] (:value op))))
                     (assoc op :type :ok, :value nil))
                 (catch [] ex
                   (assoc op :type :fail, :value nil)))
      :contains (let [ret (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "scontains") "default"] (:value op))))] (assoc op :type :ok, :value (if (nil? ret) 0 ret)))
      :size (let [ret (car/wcar {:pool {} :spec {:host conn :port 6379}} (car/redis-call (into [(str type "ssize") "default"] (:value op))))] (assoc op :type :ok, :value (if (number? ret) ret 0)))))

  (teardown! [this test])

  (close! [_ test]))

(defrecord CrdtSet [s]
  Model
  (step [this invocation]
    (condp = (.getMethodName invocation)
      "add" (CrdtSet. (conj s (int (get-arg invocation 0))))
      "remove" (CrdtSet. (disj s (int (get-arg invocation 0))))
      "contains" (if (= (contains? s (int (get-arg invocation 0))) (= 1 (int (get-ret invocation 0))))
              this
              (model/inconsistent (str "does not contain " (pr-str (int (get-arg invocation 0))))))
      "size" (if (= (count s) (int (get-ret invocation 0)))
              this
              (model/inconsistent (str "size fails"))))))

(defn crdtset
  []
  (CrdtSet. #{}))