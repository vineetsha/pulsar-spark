package org.apache.spark.streaming.pulsar;

import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.spark.internal.Logging;
import org.apache.spark.streaming.pulsar.PulsarContants;
import org.apache.spark.streaming.pulsar.PulsarProvider;
import org.apache.spark.streaming.pulsar.ViestiConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PulsarClientUtils{
    private static final Logger log = LoggerFactory.getLogger(PulsarClientUtils.class);


    private final static ViestiConfig viestiConfig=new ViestiConfig("http://10.24.20.18:80",
            "https://service.authn-prod.fkcloud.in",
            "cl-allocation-engine",
            "mRsKzAGfdKZ5Eo3ZKGpOVdQtPN4U5CnqpfU+mX9l7pw+E3MP");
    public static ImmutableMap<String, Object> PULSAR_CLIENT_PARAMS = ImmutableMap.copyOf(viestiConfig.getClientParams());

    private static ViestiConfig viestiConfigProd=new ViestiConfig("pulsar://localhost:6650",
            "https://service.authn-prod.fkcloud.in",
            "fdp-packdispatch-service",
            "bTYiIak5bDhUZ/tcaqA1lFCSOmSfh0dRkEdGtIciymefATys");
    private static Map<String, Object> clientParamsProd= viestiConfigProd.getClientParams();

    private static PulsarClient viestiClient=null;
    private static PulsarClient viestiClientProd=null;
    private static PulsarAdmin viestiAdmin=null;
    private static PulsarClient localPulsarClient=null;
    private static PulsarAdmin localAdmin=null;

    public static PulsarClient getViestiClient() throws PulsarClientException {
        if(viestiClient==null) {
            viestiClient=PulsarClient.builder()
                    .serviceUrl(PULSAR_CLIENT_PARAMS.get(PulsarContants.ServiceUrlOptionKey()).toString())
                    .authentication(PULSAR_CLIENT_PARAMS.get(PulsarContants.AuthPluginClassName()).toString(),
                            PULSAR_CLIENT_PARAMS.get(PulsarContants.AuthParams()).toString())
                    .build();
        }
        return viestiClient;
        //persistent://assets-outbound/rigel_packdispatch_service/packdispatch_service.packing_boxes
    }
    public static PulsarClient getViestiClientProd() throws PulsarClientException {
        if(viestiClientProd==null) {
            viestiClientProd=PulsarClient.builder()
                    .serviceUrl(clientParamsProd.get(PulsarContants.ServiceUrlOptionKey()).toString())
                    .authentication(clientParamsProd.get(PulsarContants.AuthPluginClassName()).toString(),
                            clientParamsProd.get(PulsarContants.AuthParams()).toString())
                    .build();
        }
        return viestiClientProd;
    }
    public static PulsarAdmin getViestiAdmin() throws PulsarClientException {
        if(viestiAdmin==null) {
            viestiAdmin=PulsarAdmin.builder()
                    .serviceHttpUrl(PULSAR_CLIENT_PARAMS.get(PulsarContants.ServiceUrlOptionKey()).toString())
                    .authentication(PULSAR_CLIENT_PARAMS.get(PulsarContants.AuthPluginClassName()).toString(),
                            PULSAR_CLIENT_PARAMS.get(PulsarContants.AuthParams()).toString())
                    .build();
        }
        return viestiAdmin;
    }

    public static PulsarClient getLocalPulsarClient() throws PulsarClientException {
        if(localPulsarClient==null) {
            localPulsarClient=PulsarClient.builder()
                    .serviceUrl(PULSAR_CLIENT_PARAMS.get(PulsarContants.ServiceUrlOptionKey()).toString())
                    .build();
        }
        return localPulsarClient;
    }
    public static PulsarAdmin getLocalPulsarAdmin(String httpServiceUrl) throws PulsarClientException {
        if(localAdmin==null) {
            localAdmin=PulsarAdmin.builder()
                    .serviceHttpUrl(httpServiceUrl)
                    .build();
        }
        return localAdmin;
    }

    public static PulsarClient getPulsarClient(Map<String, Object> clientParams) throws PulsarClientException {

         ClientBuilder builder=PulsarClient.builder();
         builder.serviceUrl(clientParams.get(PulsarContants.ServiceUrlOptionKey()).toString());
         if(clientParams.containsKey(PulsarContants.AuthPluginClassName()) && clientParams.containsKey(PulsarContants.AuthParams())){
             builder.authentication(clientParams.get(PulsarContants.AuthPluginClassName()).toString(),
                     clientParams.get(PulsarContants.AuthParams()).toString());
        }
        return builder.build();


    }
    public static <T> List<MessageId> sendMessages(PulsarClient client, Schema<T> schema, String topicName, Iterable<T> msgs) throws PulsarClientException {
        List<MessageId> mids=new ArrayList<>();
        try(Producer<T> producer=client.newProducer(schema).topic(topicName).create()){
            for(T msg: msgs){
                mids.add(producer.send(msg));
            }
        }
        return mids;
    }



    public static javafx.util.Pair<Map<String, Object>, Map<String, Object>> getClientAndReaderParams(Map<String, Object> params,String topicName, String sub, SubscriptionInitialPosition subscriptionInitialPosition){
        Map<String, Object> pulsarParams = getPulsarParams(params, topicName, sub,subscriptionInitialPosition);

        Map<String, Object> readerParams=PulsarProvider.validateReaderOptions(PulsarProvider.getReaderParams(pulsarParams));
        Map<String, Object> clientParams=PulsarProvider.validateClientParams(PulsarProvider.getClientParams(pulsarParams));
        return new Pair<>(clientParams, readerParams);
    }
    public static Map<String, Object> getPulsarParams(Map<String, Object> clientParams,String topicName, String sub, SubscriptionInitialPosition subscriptionInitialPosition){
        Map<String, Object> pulsarConf = new HashMap<>();
        pulsarConf.put("pulsar.client.serviceUrl", clientParams.get(PulsarContants.ServiceUrlOptionKey()));
        pulsarConf.put("pulsar.client.authPluginClassName", clientParams.get(PulsarContants.AuthPluginClassName()));
        pulsarConf.put("pulsar.client.authParams", clientParams.get(PulsarContants.AuthParams()));

        pulsarConf.put("pulsar.reader.subscriptionName", sub);
        pulsarConf.put("pulsar.reader.topicNames", topicName);
        pulsarConf.put("pulsar.reader.subscriptionInitialPosition", subscriptionInitialPosition);
//        pulsarConf=PulsarProvider.validateReaderOptions(pulsarConf);
        return pulsarConf;
    }

}
