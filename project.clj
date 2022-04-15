(defproject jepsen.crdt-redis "0.1.0-SNAPSHOT"
  :description "A Jepsen test for CRDT-Redis"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.crdt-redis
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.7-LOCAL"]]
  :repositories [["central" "http://maven.aliyun.com/nexus/content/groups/public"]
                 ["clojars" "https://mirrors.tuna.tsinghua.edu.cn/clojars/"]])