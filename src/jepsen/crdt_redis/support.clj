(ns jepsen.crdt-redis.support
  (:require [clojure.string :as str]
            [knossos.model :as model]
            [taoensso.carmine :as car :refer (wcar)])
  (:import [history Invocation]
           [datatype AbstractDataType]))

(def script_path "/home/ubuntu/Redis-CRDT-Experiment/experiment/redis_test/")

(def start_server "./server.sh")

(def shutdown_server "./shutdown.sh")

(def clean "./clean.sh")

(def repl "../../redis-6.0.5/src/redis-cli")

(def host_map {"10.115.119.72" 0
               "10.115.119.143" 1
               "10.115.119.199" 2})

(def port "6379")

(def local_host "127.0.0.1")

(defn all_host_port []
  (str/split (str/join " " (map #(str % " " port) (keys host_map))) #" "))

(defn parse-number [s]
  (if (nil? s)
    nil
    (Integer/parseInt s)))

(defn get-arg [invocation n]
  (.get (.getArguments invocation) n))

(defn get-ret [invocation n]
  (.get (.getRetValues invocation) n))

(defn model-transform
  [model]
  (let [ctx (atom (model))]
    (reify
      AbstractDataType
      (step [this invocation] (not (model/inconsistent? (swap! ctx model/step invocation))))
      (reset [this] (reset! ctx (model))))))