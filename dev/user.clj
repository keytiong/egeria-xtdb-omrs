(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn]))

(ctn/disable-reload!)

(apply ctn/set-refresh-dirs [(io/file "src/main/clojure")
                             (io/file "src/test/clojure")])

(defn dev []
  (require 'dev)
  (in-ns 'dev))

(defn cts []
  (require 'cts)
  (in-ns 'cts))