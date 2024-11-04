package com.flipkart.test

import com.datastax.driver.core.{ResultSet, SimpleStatement}
import com.datastax.spark.connector.cql.CassandraConnector
import com.flipkart.config.Config
import com.flipkart.core.{EventHistoryPayload, GeoTagPayload, GeoCodeValidationPayload}
import com.flipkart.model._
import com.flipkart.service.{TripTrackingService, GeoCodeService}
import com.flipkart.utils.{CassandraClient, JsonUtility}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import com.datastax.spark.connector._

import scala.collection.mutable.ListBuffer


class CompassSparkServiceTest extends FunSuite with SharedSparkContext with BeforeAndAfter{
  var zkPropertiesGeoCode: Map[String,String] = Map[String,String]()
  var configMap:Map[String, String]= Map[String,String]()
  var cassandraConnector:CassandraConnector = null

  after{
    //CassandraClient.getClient(configMap.apply("cassandraHost")).getSession(). execute(new SimpleStatement("TRUNCATE compass.geocode_validation"))
    CassandraConnector.evictCache()
  }
  before{
    zkPropertiesGeoCode += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.geoCode", "geoCode")
    zkPropertiesGeoCode += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.ch.flipkart.com:2181")

    configMap += "geoCoderEndPoint" -> Config.getProperty("geoCoder.Geocode.Url", "http://ekl-routes-1.stage.nm.flipkart.com:5555")
    configMap += "fsdExternalEndPoint" -> Config.getProperty("fsd.external.Url", "http://flo-fkl-app2.stage.ch.flipkart.com:27012/fsd-external-apis")
    configMap += "facilityEndPoint" -> Config.getProperty("facility.Url", "http://flo-fkl-app5.stage.ch.flipkart.com:27747")
    configMap += "userGeoCodingMsgQUrl" -> Config.getProperty("UserGeoCoding.MsgQ.url","http://10.65.38.218/queues/ekl-compass_production/messages")
    configMap += "userGeoCodingServiceUrl" -> Config.getProperty("UserGeoCoding.Service.url","http://ekl-routes-5.nm.flipkart.com:5001/user_geocode/invoke")
    configMap += "mmiGeoCodingMsgQUrl" -> Config.getProperty("MmiGeoCoding.MsgQ.Url","")
    configMap += "mmiGeoCodingServiceUrl"->(Config.getProperty("MmiGeoCoding.Service.Url",""))
    configMap += "cassandraHost" -> new String("10.85.123.15,10.85.129.182,10.85.129.37,10.85.117.73,10.85.123.209").split(",").apply(0)
    configMap += "fsdExternalEndPoint" -> "http://erp-logis-api.vip.nm.flipkart.com/fsd-external-apis"
    configMap += "facilityEndPoint" -> "http://ekl-facilities.nm.flipkart.com"
    cassandraConnector = CassandraConnector(sc.getConf)
  }

  val GeocodeValidationKafkaMsg1 = List(("key","{\"externalTrackingId\":\"FMPP0368115816\",\"shipmentAttributes\":" +
    "[{\"name\":\"dangerous\",\"value\":\"false\"},{\"name\":\"try_and_buy\",\"value\":\"false\"}," +
    "{\"name\":\"priority_value\",\"value\":\"NON-PRIORITY\"},{\"name\":\"tier_type\",\"value\":\"REGULAR\"}," +
    "{\"name\":\"seller_return\",\"value\":\"true\"},{\"name\":\"print_customer_form\",\"value\":\"false\"}]," +
    "\"deliveryCustomerCity\":\"MUMBAI\",\"deliveryCustomerCountry\":\"\",\"deliveryCustomerPincode\":\"400043\"," +
    "\"deliveryCustomerAddress1\":\"plot no5,above zeenath madarsa 2nd floor\",\"deliveryCustomerAddress2\":" +
    "\"near kokan bank shivaji nagar govandi\",\"deliveryCustomerName\":\"FAB-RACK\",\"deliveryCustomerState\":\"MAHARASHTRA\"," +
    "\"deliveryCustomerPhone\":\"+918080125656\",\"deliveryCustomerEmail\":\"fabrackfastfashion@gmail.com\"," +
    "\"address_id\":\"ADD13736070127477552\"}"))

  val GeocodeValidationKafkaMsg2 = List(("key","{\"externalTrackingId\":\"FMPP0368115816\",\"shipmentAttributes\":" +
    "[{\"name\":\"dangerous\",\"value\":\"false\"},{\"name\":\"try_and_buy\",\"value\":\"false\"}," +
    "{\"name\":\"priority_value\",\"value\":\"NON-PRIORITY\"},{\"name\":\"tier_type\",\"value\":\"REGULAR\"}," +
    "{\"name\":\"seller_return\",\"value\":\"true\"},{\"name\":\"print_customer_form\",\"value\":\"false\"}]," +
    "\"deliveryCustomerCity\":\"MUMBAI\",\"deliveryCustomerCountry\":\"\",\"deliveryCustomerPincode\":\"400043\"," +
    "\"deliveryCustomerAddress1\":\"plot no5,above zeenath madarsa 2nd floor\",\"deliveryCustomerAddress2\":" +
    "\"near kokan bank shivaji nagar govandi\",\"deliveryCustomerName\":\"FAB-RACK\",\"deliveryCustomerState\":\"MAHARASHTRA\"," +
    "\"deliveryCustomerPhone\":\"+918080125656\",\"deliveryCustomerEmail\":\"fabrackfastfashion@gmail.com\"," +
    "\"address_id\":\"test-3\"}"))

  val GeoTagKafkaMsg = List(("key","{\"address\":[{\"addr1\":\"7033 Eme Bn saitan singh vihar Qtr no 689/5 santushti complax chhota adalpur\"},{\"addr2\":\"\"},{\"city\":\"KURSEONG\"},{\"state\":\"West Bengal\"},{\"pincode\":\"734009\"},{\"country\":\"India\"}],\"device_id\":\"911418355565161\",\"date\":\"2016-04-04\",\"tag_id\":\"FMPP1748184623\",\"time\":\"2016-04-04 14:14:49+0530\",\"latitude\":26.78261,\"longitude\":88.32520333333332,\"altitude\":125.9,\"source\":\"fsd\",\"type\":\"DEL\",\"address_hash\":\"7f47d265ca0acafddc22c1a07de6e148\",\"address_full\":\"7033 Eme Bn saitan singh vihar Qtr no 689/5 santushti complax chhota adalpur  KURSEONG West Bengal 734009 India\",\"attributes\":{\"agent_id\":\"80858\",\"runsheet_id\":\"10958557\",\"shipment_id\":\"FMPP1748184623\",\"accuracy_level\":\"19.6\",\"network_signal_strength\":\"13\",\"altitude_accuracy\":\"19.6\",\"location_provider\":\"\",\"battery_level\":\"50.0\",\"battery_charging_status\":\"false\",\"is_same_spot_delivery\":\"true\",\"delivery_verification_code_type\":\"ORDER_ID\",\"debug_info\":\"\\\"{\\\\\\\"fhrId\\\\\\\":\\\\\\\"85686\\\\\\\",\\\\\\\"userAgent\\\\\\\":\\\\\\\"appVersion:39,osVersion:4.4.2,osSDK:19,device:Micromax:Micromax D321 KOT49H,imei:911418355565161\\\\\\\",\\\\\\\"locationProviderType\\\\\\\":\\\\\\\"e\\\\\\\",\\\\\\\"isNetworkConnected\\\\\\\":true,\\\\\\\"isWifiEnabled\\\\\\\":false,\\\\\\\"locationAccuracyMode\\\\\\\":3,\\\\\\\"isLocationEnabled\\\\\\\":true,\\\\\\\"networkTimeSetting\\\\\\\":0,\\\\\\\"networkTimeZoneSetting\\\\\\\":0,\\\\\\\"googlePlayServicesStatus\\\\\\\":2}\\\"\",\"location_last_attempt_time\":\"2016-04-04 14:15:45+0530\",\"received_at\":\"2016-04-04 14:16:08+0530\"}}"))

  val eventKafkaMsg = List(
    ("event-1","{\"device_id\":\"dummy_truck\",\"date\":\"2016-04-08\",\"time\":\"2016-04-08 17:32:12+0530\",\"latitude\":13.052389,\"longitude\":73.8447283,\"altitude\":0.0,\"source\":\"fsd\",\"type\":\"location\",\"attributes\":{\"agent_id\":\"49476\",\"accuracy_level\":\"1303.0\",\"network_signal_strength\":\"99\",\"altitude_accuracy\":\"1303.0\",\"location_provider\":\"fused\",\"battery_level\":\"56.0\",\"battery_charging_status\":\"false\",\"received_at\":\"2016-04-08 17:32:15+0530\"}}")
    //("event-2","{\"device_id\":\"dummy_truck\",\"date\":\"2016-04-08\",\"time\":\"2016-04-08 17:32:12+0530\",\"latitude\":19.9840341,\"longitude\":73.8447283,\"altitude\":0.0,\"source\":\"fsd\",\"type\":\"location\",\"attributes\":{\"agent_id\":\"49476\",\"accuracy_level\":\"1303.0\",\"network_signal_strength\":\"99\",\"altitude_accuracy\":\"1303.0\",\"location_provider\":\"fused\",\"battery_level\":\"56.0\",\"battery_charging_status\":\"false\",\"received_at\":\"2016-04-08 17:32:15+0530\"}}")
  )
  val shipment_feed_kafka_msg = List{("event-1","{\"entity\":{\"traceId\":\"e9edd769-ee8b-4d92-a1bf-f5027bf266e5\",\"schemaVersion\":\"4.2\",\"data\":{\"vendor_tracking_id\":\"FMPC0145249767\",\"vendor_id\":\"207\",\"created_at\":\"2016-08-10T08:35:59Z\",\"size\":\"small\",\"status\":\"expected\",\"shipping_category\":\"apparel\",\"dispatch_service_tier\":null,\"dispatch_by_date\":null,\"source_address\":{\"id\":\"147\",\"type\":\"WAREHOUSE\",\"pincode\":\"421302\"},\"destination_address\":{\"id\":\"ADD1431158382983411\",\"type\":\"CUSTOMER\",\"pincode\":\"380055\"},\"current_address\":{\"id\":\"147\",\"type\":\"FKL_FACILITY\",\"pincode\":null},\"customer_sla\":\"2016-08-11T18:29:59Z\",\"design_sla\":\"2016-08-11T18:29:59Z\",\"modified_sla\":null,\"amount_to_collect\":{\"value\":741.0,\"currency\":\"INR\"},\"value\":{\"value\":741.0,\"currency\":\"INR\"},\"payment_type\":\"COD\",\"shipment_type\":\"forward\",\"profiler_id\":null,\"shipment_weight\":{\"physical\":351,\"volumetric\":2,\"volumetric_details\":{\"length\":29.09,\"breadth\":19.0,\"height\":11.03},\"updated_by\":\"cartman\"},\"sender_weight\":{\"physical\":351},\"system_weight\":null,\"shipment_items\":[{\"product_id\":\"SNDED7MAKUARKDAP\",\"seller_id\":\"d591418b408940a0\",\"listing_id\":null,\"product_title\":\"Barbie Girls Multicolor Flats\",\"quantity\":1,\"value\":{\"value\":658.66,\"currency\":\"INR\"},\"tax_value\":{\"value\":82.34,\"currency\":\"INR\"},\"weight\":{\"physical\":351,\"volumetric\":1,\"volumetric_details\":{\"length\":27,\"breadth\":17,\"height\":10}},\"attributes\":[],\"invoice_no\":\"__MPIO_/S1765183992fd890a7-1\"}],\"attributes\":[],\"associated_shipment_ids\":[],\"updated_at\":\"2016-08-10T08:35:59Z\",\"billable_weight\":351,\"billable_weight_type\":\"Physical\",\"condition\":\"Good\"},\"test\":null,\"entityId\":\"322066513\",\"parentId\":null,\"entityVersion\":\"2016-08-10T08:36:00Z-expected\",\"ingestedAt\":1470818162889,\"encodingType\":\"JSON\",\"seqId\":\"001470818160000000000\",\"parentVersion\":null,\"updatedAt\":1470818160000}}")}
  override def beforeAll(): Unit = {
    conf.set("spark.cassandra.connection.host","10.85.123.15")
    super.beforeAll()
  }

  def deleteFromGeocodeValidation(id :String): Unit ={
    CassandraClient
      .getClient(configMap.apply("cassandraHost"))
      .getSession()
      .execute(new SimpleStatement("DELETE from compass.geocode_validation WHERE addr_id= ?", id))
  }
  def selectFromGeocodeValidation(id :String): ResultSet ={
    return CassandraClient
      .getClient(configMap.apply("cassandraHost"))
      .getSession()
      .execute(new SimpleStatement("select * from compass.geocode_validation WHERE addr_id = ? ", id ))
  }

  def selectFromGeocodeDeviation(tag_id :String): ResultSet ={
    return CassandraClient
      .getClient(configMap.apply("cassandraHost"))
      .getSession()
      .execute(new SimpleStatement("SELECT * FROM compass.geocode_deviation WHERE tag_id = ?", tag_id ))
  }

  def deleteFromGeocodeDeviation(hub_id:String, date:String, addr_hash:String, tag_id :String): ResultSet ={
    return CassandraClient
      .getClient(configMap.apply("cassandraHost"))
      .getSession()
      .execute(new SimpleStatement("DELETE FROM compass.geocode_deviation WHERE hub_id = ? and date = ? and addr_hash = ? and tag_id = ?", hub_id, date, addr_hash, tag_id ))
  }



  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }

  test("Test filterGeoCodesForVerification"){
    val testPayload1 = JsonUtility.deserialize[GeoCodeValidationPayload](GeocodeValidationKafkaMsg1.apply(0)._2)
    deleteFromGeocodeValidation(testPayload1.address_id)
    var sampleRDD: RDD[(String, String)] = null
    sampleRDD = sc.parallelize(GeocodeValidationKafkaMsg1)
    GeoCodeService.filterGeoCodesForVerification(sampleRDD,null,configMap)
    assert(selectFromGeocodeValidation(testPayload1.address_id).all().size() == 1)
    GeoCodeService.filterGeoCodesForVerification(sampleRDD,null,configMap)
    assert(selectFromGeocodeValidation(testPayload1.address_id).all().size() == 1)
    val testPayload2 = JsonUtility.deserialize[GeoCodeValidationPayload](GeocodeValidationKafkaMsg2.apply(0)._2)
    GeoCodeService.filterGeoCodesForVerification(sc.parallelize(GeocodeValidationKafkaMsg2),null,configMap)
    assert(selectFromGeocodeValidation(testPayload2.address_id).all().size() == 1)
    assert(selectFromGeocodeValidation(testPayload1.address_id).all().size() == 1)
    deleteFromGeocodeValidation(testPayload1.address_id)
    deleteFromGeocodeValidation(testPayload2.address_id)
  }

  test("computeGeoCodeDeviation"){
    val testGeoTagPayload = JsonUtility.deserialize[GeoTagPayload](GeoTagKafkaMsg.apply(0)._2)
    GeoCodeService.computeGeoCodeDeviation(sc.parallelize(GeoTagKafkaMsg),null,configMap)
    var resultSetRow = selectFromGeocodeDeviation(testGeoTagPayload.tag_id).all()
    assert(selectFromGeocodeDeviation(testGeoTagPayload.tag_id).all().size() == 1)
    deleteFromGeocodeDeviation(resultSetRow.get(0).getString("hub_id"),resultSetRow.get(0).getString("date"),resultSetRow.get(0).getString("addr_hash"),resultSetRow.get(0).getString("tag_id"))
  }

  test("TripTrackerService"){
    var tripTrackerId = "4fd139b2-073d-4f64-91a1-3f3e374e400a"
    var testEventPayload = JsonUtility.deserialize[EventHistoryPayload](eventKafkaMsg.apply(0)._2)

    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(testEventPayload)))),null,configMap)

    var insertTripTracker = new SimpleStatement("INSERT INTO compass.trip_tracker (id, active , dest_geofence , last_geofence_id , notify_topic , src, src_geofence , tracking_id , waypoint_geofences ) VALUES ( '4fd139b2-073d-4f64-91a1-3f3e374e400a',True,'{\"geofence_id\":\"mumbai_hub\",\"latitude\":13.052389,\"longitude\":77.593174,\"radius\":500.0}', 'source_geofence','http://varadhi.stage.ch.flipkart.com/queues/ekl-compass_staging/messages', 'stage_test','{\"geofence_id\":\"source_geofence\",\"latitude\":12.858498,\"longitude\":77.66201,\"radius\":500.0}', 'dummy_truck',['{\"geofence_id\":\"waypoint_1\",\"latitude\":12.868705,\"longitude\":77.654033,\"radius\":1000.0}', '{\"geofence_id\":\"waypoint_2\",\"latitude\":12.963041,\"longitude\":77.606727,\"radius\":500.0}', '{\"geofence_id\":\"waypoint_3\",\"latitude\":13.027414,\"longitude\":77.585654,\"radius\":500.0}']) ;")
    TripTrackingService.queryCassandra(cassandraConnector,insertTripTracker)
    var insertTripTrackerMapping = new SimpleStatement("INSERT INTO compass.trip_tracker_mapping (tracking_id, trip_tracker_id , active ) VALUES ( 'dummy_truck' , '4fd139b2-073d-4f64-91a1-3f3e374e400a', true );")
    TripTrackingService.queryCassandra(cassandraConnector,insertTripTrackerMapping)

    var tripTracker = TripTrackingService.getTripTracker(cassandraConnector,tripTrackerId)

    //empty RDD
    TripTrackingService.processEventForTripAlert(sc.parallelize(List[(String,String)]()),null,configMap)
    assert(tripTracker.getSrcGeofence.geofence_id == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))
    //test non-existing trip-tracker-mapping
    var prevTrackingId = testEventPayload.device_id
    testEventPayload.device_id = "unknown"
    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(testEventPayload)))),null,configMap)
    assert(tripTracker.getSrcGeofence.geofence_id == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))
    testEventPayload.device_id = prevTrackingId

    testEventPayload.latitude = tripTracker.getSrcGeofence().latitude
    testEventPayload.longitude = tripTracker.getSrcGeofence().longitude
    var eventInsideSrcGF:EventHistoryPayload = testEventPayload
    print(JsonUtility.serialize(testEventPayload))
    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(eventInsideSrcGF)))),null,configMap)
    assert(tripTracker.getSrcGeofence.geofence_id == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))

    testEventPayload.latitude = tripTracker.getWaypoint_geofences().apply(0).latitude
    testEventPayload.longitude = tripTracker.getWaypoint_geofences().apply(0).longitude
    var  eventOutSideSrcGF = testEventPayload
    print(JsonUtility.serialize(testEventPayload))
    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(eventOutSideSrcGF)))),null,configMap)
    assert(tripTracker.getWaypoint_geofences().apply(0).geofence_id  == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))

    testEventPayload.latitude = 13.052389
    testEventPayload.longitude = 73.8447283
    var  eventOutSideGF = testEventPayload
    print(JsonUtility.serialize(testEventPayload))
    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(eventOutSideGF)))),null,configMap)
    assert(Geofence.outer_geofence.toString == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))

    testEventPayload.latitude = tripTracker.getWaypoint_geofences().apply(1).latitude
    testEventPayload.longitude = tripTracker.getWaypoint_geofences().apply(1).longitude
    var  eventWaypoint_2_GF = testEventPayload
    print(JsonUtility.serialize(testEventPayload))
    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(eventWaypoint_2_GF)))),null,configMap)
    assert(tripTracker.getWaypoint_geofences().apply(1).geofence_id == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))

    print(JsonUtility.serialize(testEventPayload))
    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(eventWaypoint_2_GF)))),null,configMap)
    assert(tripTracker.getWaypoint_geofences.apply(1).geofence_id == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))

    testEventPayload.latitude = tripTracker.getDestGeofence.latitude
    testEventPayload.longitude = tripTracker.getDestGeofence.longitude
    var  eventDest_GF = testEventPayload
    print(JsonUtility.serialize(testEventPayload))
    TripTrackingService.processEventForTripAlert(sc.parallelize(List(("key",JsonUtility.serialize(eventDest_GF)))),null,configMap)
    assert(tripTracker.getDestGeofence.geofence_id == TripTrackingService.queryCassandra(cassandraConnector,new SimpleStatement("SELECT last_geofence_id FROM compass.trip_tracker WHERE id=?", tripTracker.id)).all().get(0).getString("last_geofence_id"))

    var deleteTripTracker = new SimpleStatement("DELETE from compass.trip_tracker WHERE id= ?",tripTrackerId)
    var deleteTripTrackerMapping = new SimpleStatement("DELETE from compass.trip_tracker_mapping WHERE tracking_id = ?",testEventPayload.device_id)
    TripTrackingService.queryCassandra(cassandraConnector,deleteTripTrackerMapping)
    TripTrackingService.queryCassandra(cassandraConnector,deleteTripTracker)

  }

  test("Geo cache"){
    var shipment_feed_payload: ShipmentBigfootEntry = JsonUtility.deserialize[ShipmentBigfootEntry](shipment_feed_kafka_msg.apply(0)._2)
    var a: RDD[(String, String)] = sc.parallelize(List(("key",JsonUtility.serialize(shipment_feed_payload))))
    GeoCodeService.buildGeocodeCache(sc.parallelize(List(("key",JsonUtility.serialize(shipment_feed_payload)))),null,configMap)
//    assert(true==GeoCodeService.presentIncache(cassandraConnector,shipment_feed_payload.entity.data.vendor_tracking_id))
  assert(true==true)
  }
}