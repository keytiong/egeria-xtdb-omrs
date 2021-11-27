(ns cts
  (:require [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.core :as ig]
            [clj-http.client :as http]
            [clojure.java.io :as io]
            [clojure.data.json :as json])
  (:import [org.testcontainers.containers KafkaContainer]
           [org.testcontainers.utility DockerImageName]))

(def config
  {::event-bus  {}

   ::platform   {:version       "3.3"
                 :egeria-home   "/opt/egeria/egeria-omag"
                 :server-port   9443
                 :admin-user-id "garygeeke"}

   ::sut-server {:admin-platform                (ig/ref ::platform)
                 :platform                      (ig/ref ::platform)
                 :server-user-id                "SUTnpa"
                 :server-password               "SUTpassw0rd"
                 :server-name                   "SUT_Server"
                 :server-type                   "Metadata Repository Server"
                 :event-bus                     (ig/ref ::event-bus)
                 :cohort                        "devCohort"
                 :metadata-collection-name      "SUT_MDR"
                 :metadata-repository-type      "repository-proxy"
                 :metadata-repository-connector {:connector-provider    "io.kosong.egeria.omrs.XtdbOMRSRepositoryConnectorProvider"
                                                 :additional-properties {}}
                 }

   ::cts-server {:admin-platform           (ig/ref ::platform)
                 :platform                 (ig/ref ::platform)
                 :sut-server               (ig/ref ::sut-server)
                 :server-user-id           "CTS1npa"
                 :server-password          "CTS1passw0rd"
                 :server-name              "CTS_Server"
                 :server-type              "Conformance Suite Server"
                 :event-bus                (ig/ref ::event-bus)
                 :cohort                   "devCohort"
                 :metadata-repository-type "in-memory-repository"}})

(integrant.repl/set-prep! (constantly config))

(defn system []
  integrant.repl.state/system)

(defn admin-command-url [{:keys [admin-user-id
                                 platform-url]}]
  (str platform-url "/open-metadata/admin-services/users/" admin-user-id "/servers/"))

(defn origin [platform]
  (http/get (str (:platform-url platform) "/open-metadata/platform-services/users/garygeeke/server-platform/origin/")
    {:insecure? true}))

(defn configure-platform-url
  [admin-platform server-name target-platform]
  (let [admin-command-url   (admin-command-url admin-platform)
        url                 (str admin-command-url server-name "/server-url-root")
        target-platform-url (:platform-url target-platform)]
    (http/post url
      {:query-params {"url" target-platform-url}
       :insecure?    true})))

(defn configure-server-type
  [admin-platform server-name server-type]
  (let [admin-command-url (admin-command-url admin-platform)
        url               (str admin-command-url server-name "/server-type")]
    (http/post url
      {:query-params {"typeName" server-type}
       :insecure?    true})))

(defn configure-user-id
  [admin-platform server-name user-id]
  (let [admin-command-url (admin-command-url admin-platform)
        url               (str admin-command-url server-name "/server-user-id")]
    (http/post url
      {:query-params {"id" user-id}
       :insecure?    true})))

(defn configure-password
  [admin-platform server-name password]
  (let [admin-command-url (admin-command-url admin-platform)
        url               (str admin-command-url server-name "/server-user-password")]
    (http/post url
      {:query-params {"password" password}
       :insecure?    true})))

(defn configure-metadata-repository
  [admin-platform server-name repository-type]
  (let [admin-command-url (admin-command-url admin-platform)
        url               (str admin-command-url server-name "/local-repository/mode/" repository-type)]
    (http/post url
      {:insecure? true})))

(defn configure-plugin-metadata-repository
  [admin-platform server-name metadata-repository-connector]
  (let [admin-command-url          (admin-command-url admin-platform)
        connector-provider         (:connector-provider metadata-repository-connector)
        additional-properties      (:additional-properties metadata-repository-connector)
        url                        (str admin-command-url server-name "/local-repository/mode/plugin-repository/details")
        query-params               {:connectorProvider connector-provider}
        additional-properties-body (json/json-str additional-properties)]
    (http/post url
      {:insecure?    true
       :content-type :json
       :query-params query-params
       :body         additional-properties-body})))

(defn configure-descriptive-name
  [admin-platform server-name collection-name]
  (let [admin-command-url (admin-command-url admin-platform)
        url               (str admin-command-url server-name "/local-repository/metadata-collection-name/" collection-name)]
    (http/post url
      {:insecure? true})))

(defn configure-event-bus
  [admin-platform server-name event-bus-root-url]
  (let [admin-command-url (admin-command-url admin-platform)
        url               (str admin-command-url server-name "/event-bus")
        event-bus-body    (json/json-str
                            {"producer" {"bootstrap.servers" event-bus-root-url}
                             "consumer" {"bootstrap.servers" event-bus-root-url}})]
    (http/post url
      {:insecure?    true
       :content-type :json
       :body         event-bus-body})))

(defn configure-cohort-membership
  [admin-platform server-name cohort-name]
  (let [admin-command-url (admin-command-url admin-platform)
        url               (str admin-command-url server-name "/cohorts/" cohort-name)]
    (http/post url
      {:insecure? true})))

(defn configure-repository-workbench
  [admin-platform server-name sut-server-name]
  (let [admin-command-url     (admin-command-url admin-platform)
        url                   (str admin-command-url server-name "/conformance-suite-workbenches/repository-workbench/repositories")
        workbench-config-body (json/json-str
                                {"class"                   "RepositoryConformanceWorkbenchConfig"
                                 "tutRepositoryServerName" sut-server-name
                                 "maxSearchResults"        10})]
    (http/post url
      {:insecure?    true
       :content-type :json
       :body         workbench-config-body})))

(defn deploy-server
  [admin-platform server-name target-platform]
  (let [admin-command-url   (admin-command-url admin-platform)
        url                 (str admin-command-url server-name "/configuration/deploy")
        target-platform-url (:platform-url target-platform)
        request-body        (json/json-str
                              {"class"   "URLRequestBody"
                               "urlRoot" target-platform-url})]
    (http/post url
      {:insecure?    true
       :content-type :json
       :body         request-body})))

(defn start-server
  [platform server-name]
  (let [platform-url  (:platform-url platform)
        admin-user-id (:admin-user-id platform)
        url           (str platform-url "/open-metadata/admin-services/users/" admin-user-id "/servers/" server-name "/instance")]
    (http/post url
      {:insecure? true})))

(defn stop-server
  [platform server-name]
  (let [platform-url  (:platform-url platform)
        admin-user-id (:admin-user-id platform)
        url           (str platform-url "/open-metadata/admin-services/users/" admin-user-id "/servers/" server-name "/instance")]
    (http/delete url
      {:insecure? true})))

(defn start-sut-server []
  (let [system      (system)
        platform    (::platform system)
        server-name (get-in system [::sut-server :server-name])]
    (start-server platform server-name)))

(defn start-cts-server []
  (let [system      (system)
        platform    (::platform system)
        server-name (get-in system [::cts-server :server-name])]
    (start-server platform server-name)))


(defn start-kafka-container []
  (let [kafka    (doto (KafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:5.4.3"))
                   (.withExposedPorts (into-array Integer [(int 9093)]))
                   (.start))
        root-url (str "localhost:" (.getMappedPort kafka 9093))]
    {:kafka-container kafka
     :root-url        root-url}))

(defn start-omag-platform [{:keys [egeria-home version server-port admin-user-id]}]
  (let [server-jar-path  (str egeria-home "/server/server-chassis-spring-" version ".jar")
        jvm-opts         (str "-Dserver.port=" server-port)
        process-builder  (doto (ProcessBuilder. ["/usr/bin/java" jvm-opts "-jar" server-jar-path])
                           (.directory (io/file egeria-home)))
        platform-process (.start process-builder)
        platform-url     (str "https://localhost:" server-port)]
    (Thread/sleep 15000)
    {:platform-process platform-process
     :platform-url     platform-url
     :admin-user-id    admin-user-id}))

(defmethod ig/init-key ::event-bus [_ _]
  (start-kafka-container))

(defmethod ig/halt-key! ::event-bus [_ {:keys [^KafkaContainer kafka-container]}]
  (when kafka-container
    (.stop kafka-container)))

(defmethod ig/init-key ::platform [_ config]
  (start-omag-platform config))

(defmethod ig/halt-key! ::platform [_ {:keys [^Process platform-process]}]
  (when platform-process
    (.destroy platform-process)))

(defmethod ig/init-key ::sut-server
  [_ {:keys [admin-platform server-name event-bus server-user-id server-password server-type
             platform metadata-repository-connector metadata-collection-name
             cohort metadata-repository-connector-provider additional-properties]
      :as   server-config}]
  (let [event-bus-root-url (:root-url event-bus)]
    (configure-platform-url admin-platform server-name platform)
    (configure-server-type admin-platform server-name server-type)
    (configure-user-id admin-platform server-name server-user-id)
    (configure-password admin-platform server-name server-password)
    (configure-plugin-metadata-repository admin-platform server-name metadata-repository-connector)
    ;;(configure-metadata-repository admin-platform server-name metadata-repository-type)
    (configure-descriptive-name admin-platform server-name metadata-collection-name)
    (configure-event-bus admin-platform server-name event-bus-root-url)
    (configure-cohort-membership admin-platform server-name cohort)
    (deploy-server admin-platform server-name platform)
    #_(start-server platform server-name)
    server-config))

(defmethod ig/halt-key! ::sut-server [_ {:keys [platform server-name]}]
  (stop-server platform server-name))

(defmethod ig/init-key ::cts-server
  [_ {:keys [admin-platform server-name event-bus server-user-id server-password server-type
             platform metadata-repository-type sut-server
             cohort]
      :as   server-config}]
  (let [event-bus-root-url (:root-url event-bus)
        sut-server-name    (:server-name sut-server)]
    (configure-platform-url admin-platform server-name platform)
    (configure-server-type admin-platform server-name server-type)
    (configure-user-id admin-platform server-name server-user-id)
    (configure-password admin-platform server-name server-password)
    (configure-metadata-repository admin-platform server-name metadata-repository-type)
    (configure-event-bus admin-platform server-name event-bus-root-url)
    (configure-cohort-membership admin-platform server-name cohort)
    (configure-repository-workbench admin-platform server-name sut-server-name)
    (deploy-server admin-platform server-name platform)
    #_(start-server platform server-name)
    server-config))

(defmethod ig/halt-key! ::cts-server [_ {:keys [platform server-name]}]
  (stop-server platform server-name))

;; https://localhost:9443/servers/CTS_Server/open-metadata/conformance-suite/users/garygeeke/report/summary

;; https://localhost:9443/servers/CTS_Server/open-metadata/conformance-suite/users/garygeeke/report/test-cases/failed