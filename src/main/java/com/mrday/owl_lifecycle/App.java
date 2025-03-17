package com.mrday.owl_lifecycle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.UUID;

public class App {
    // Endpoint di update di Fuseki (modifica se necessario)
    private static final String FUSEKI_UPDATE_ENDPOINT = "http://localhost:3030/defaultOpenapi/update";
    // Namespace base per l'ontologia
    private static final String NS = "http://example.org/ontology#";

    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/persistData", new PersistDataHandler());
        server.createContext("/stop", new StopHandler(server));
        server.setExecutor(null);
        server.start();
        System.out.println("Server in ascolto su http://localhost:8000/persistData");
        System.out.println("Per stoppare il server, invia una richiesta GET a http://localhost:8000/stop");
    }

    static class PersistDataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            InputStream is = exchange.getRequestBody();
            String jsonData = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines().reduce("", (acc, line) -> acc + line);
            is.close();
            try {
                Model model = convertJsonToRdf(jsonData);
                StringWriter writer = new StringWriter();
                model.write(writer, "N-TRIPLE");
                String rdfData = writer.toString();
                // Crea la query SPARQL UPDATE (senza includere l'URL)
                String sparqlUpdate = "INSERT DATA { " + rdfData + " }";
                
                // Stampa in console la query che verrà inviata
                System.out.println("Query SPARQL da inviare:");
                System.out.println(sparqlUpdate);
                
                UpdateRequest updateRequest = UpdateFactory.create(sparqlUpdate);
                UpdateProcessor proc = UpdateExecutionFactory.createRemote(updateRequest, FUSEKI_UPDATE_ENDPOINT);
                proc.execute();
                String response = "Dati persistiti con successo nel Knowledge Graph.";
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (Exception e) {
                e.printStackTrace();
                String errorResponse = "Errore durante la persistenza: " + e.getMessage();
                exchange.sendResponseHeaders(500, errorResponse.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(errorResponse.getBytes());
                os.close();
            }
        }

        /**
         * Converte un JSON in RDF secondo le regole:
         * - Il nome esterno (chiave) diventa la classe principale.
         * - Se il nodo contiene "results" o "data", ogni elemento dell'array diventa
         *   un'istanza di tipo {nomeClasse}_results o {nomeClasse}_data, collegata alla
         *   risorsa principale tramite la proprietà "results" o "data".
         * - Altrimenti, se il nodo è un oggetto, viene processato come istanza di tipo {nomeClasse}.
         */
        private static Model convertJsonToRdf(String jsonData) throws IOException {
            Model model = ModelFactory.createDefaultModel();
            model.setNsPrefix("ex", NS);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(jsonData);
            Iterator<String> classKeys = root.fieldNames();
            while (classKeys.hasNext()) {
                String mainClassName = classKeys.next();
                JsonNode mainNode = root.get(mainClassName);
                // Il tipo della classe principale è NS + mainClassName
                Resource mainClassType = model.createResource(NS + mainClassName);
                // Se il nodo contiene "results"
                if (mainNode.has("results") && mainNode.get("results").isArray()) {
                    // Crea l'istanza principale per il JSON (con URI fittizio)
                    Resource mainInstance = model.createResource(NS + mainClassName + "_Instance");
                    mainInstance.addProperty(RDF.type, mainClassType);
                    // Per ogni elemento dell'array, processa come istanza di tipo {mainClassName}_results
                    for (JsonNode inst : mainNode.get("results")) {
                        Resource subClassType = model.createResource(NS + mainClassName + "_results");
                        Resource instance = processInstance(model, mainClassName + "_results", subClassType, inst, NS + mainClassName + "_Instance");
                        Property prop = model.createProperty(NS, "results");
                        mainInstance.addProperty(prop, instance);
                    }
                }
                // Se il nodo contiene "data"
                else if (mainNode.has("data") && mainNode.get("data").isArray()) {
                    Resource mainInstance = model.createResource(NS + mainClassName + "_Instance");
                    mainInstance.addProperty(RDF.type, mainClassType);
                    for (JsonNode inst : mainNode.get("data")) {
                        Resource subClassType = model.createResource(NS + mainClassName + "_data");
                        Resource instance = processInstance(model, mainClassName + "_data", subClassType, inst, NS + mainClassName + "_Instance");
                        Property prop = model.createProperty(NS, "data");
                        mainInstance.addProperty(prop, instance);
                    }
                }
                // Altrimenti, se il nodo è un oggetto, processalo come istanza di tipo mainClassName
                else if (mainNode.isObject()) {
                    processInstance(model, mainClassName, mainClassType, mainNode, null);
                }
            }
            return model;
        }

        /**
         * Processa ricorsivamente un'istanza (o frammento) JSON in RDF.
         *
         * Regole:
         * - Se il nodo contiene un campo "results" o "data", per ciascun elemento dell'array
         *   viene creato un sotto-oggetto di tipo {nomeClassePadre}_{results|data}.
         * - I campi con valori semplici diventano proprietà (literal).
         * - I campi che contengono oggetti diventano nuove istanze (con classe uguale al nome del campo)
         *   e sono collegate all'istanza padre tramite la proprietà avente lo stesso nome.
         *
         * @param model       Il modello RDF.
         * @param className   Il nome (per URI) da usare per questa istanza.
         * @param classType   La risorsa che rappresenta il tipo della classe.
         * @param instanceNode Il nodo JSON da processare.
         * @param parentUri   (Opzionale) URI del nodo padre, usato per costruire in modo gerarchico l'URI.
         * @return La risorsa RDF creata per questa istanza.
         */
        private static Resource processInstance(Model model, String className, Resource classType, JsonNode instanceNode, String parentUri) {
            String instanceUri;
            if (instanceNode.has("id")) {
                String idValue = instanceNode.get("id").asText();
                instanceUri = NS + className + "_" + idValue;
            } else {
                // Se non è presente "id", genera un URI (eventualmente gerarchico)
                if (parentUri == null) {
                    instanceUri = NS + className + "_" + UUID.randomUUID();
                } else {
                    instanceUri = parentUri + "_" + className + "_" + UUID.randomUUID();
                }
            }
            Resource instance = model.createResource(instanceUri);
            instance.addProperty(RDF.type, classType);

            Iterator<String> fields = instanceNode.fieldNames();
            while (fields.hasNext()) {
                String prop = fields.next();
                // I campi "id", "results" e "data" sono gestiti separatamente
                if ("id".equals(prop))
                    continue;
                JsonNode valueNode = instanceNode.get(prop);
                // Se il campo è "results" o "data", processa ogni elemento come sottostruttura
                if ("results".equals(prop) || "data".equals(prop)) {
                    if (valueNode.isArray()) {
                        for (JsonNode arrElem : valueNode) {
                            // Il tipo della sottostruttura sarà NS + {nomeClassePadre}_{results|data}
                            Resource subClassType = model.createResource(NS + className + "_" + prop);
                            Resource subInstance = processInstance(model, className + "_" + prop, subClassType, arrElem, instanceUri);
                            Property property = model.createProperty(NS, prop);
                            instance.addProperty(property, subInstance);
                        }
                    }
                    continue;
                }
                // Se il valore è semplice (literal), lo aggiunge come proprietà
                if (valueNode.isValueNode()) {
                    Property property = model.createProperty(NS, prop);
                    instance.addProperty(property, valueNode.asText());
                }
                // Se il valore è un oggetto, lo processa ricorsivamente
                else if (valueNode.isObject()) {
                    Resource nestedClassType = model.createResource(NS + prop);
                    Resource nestedInstance = processInstance(model, prop, nestedClassType, valueNode, instanceUri);
                    Property property = model.createProperty(NS, prop);
                    instance.addProperty(property, nestedInstance);
                }
                // Se il valore è un array (di valori semplici o oggetti)
                else if (valueNode.isArray()) {
                    // Se tutti gli elementi sono valori semplici, aggiunge multipli literal
                    boolean allValuesSimple = true;
                    for (JsonNode elem : valueNode) {
                        if (!elem.isValueNode()) {
                            allValuesSimple = false;
                            break;
                        }
                    }
                    if (allValuesSimple) {
                        Property property = model.createProperty(NS, prop);
                        for (JsonNode elem : valueNode) {
                            instance.addProperty(property, elem.asText());
                        }
                    } else {
                        // Se l'array contiene oggetti, processa ciascun oggetto come sottostruttura
                        for (JsonNode elem : valueNode) {
                            if (elem.isObject()) {
                                Resource nestedClassType = model.createResource(NS + prop);
                                Resource nestedInstance = processInstance(model, prop, nestedClassType, elem, instanceUri);
                                Property property = model.createProperty(NS, prop);
                                instance.addProperty(property, nestedInstance);
                            }
                        }
                    }
                }
            }
            return instance;
        }
    }

    static class StopHandler implements HttpHandler {
        private final HttpServer server;
        public StopHandler(HttpServer server) { this.server = server; }
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String response = "Server stoppato.";
            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
            System.out.println("Ricevuta richiesta per stoppare il server. Arresto in corso...");
            server.stop(0);
        }
    }
}
