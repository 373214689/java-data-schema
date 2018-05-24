package com.liuyang.test;

import com.liuyang.data.DataSet;
import com.liuyang.data.Row;
import com.liuyang.data.util.Schema;

public class DataSetTest {

	public static void main(String[] args) {
		double s = System.nanoTime();
		Schema schema = getHttpSchema();
		DataSet ds = new DataSet(schema);
		
		System.out.println(ds.schema());
		/*ds.readTextFile(line -> Row.parseLine(schema, line, "|"), true
				,"d:/test/LTE_GZ_YDGZG00499_103000036816_20180220000000.txt"
		);*/
		ds.readTextFile(line -> ds.createRow().parseLine(line, "|"), false
				,"d:/test/LTE_GZ_YDGZG00499_103000036816_20180220000000.txt"
		);

		System.out.println("read used: " + String.format("%.2fms", (System.nanoTime() - s) / 1000000));
		System.out.println("dataset size: " + ds.size());
		System.out.println(ds);


		s = System.nanoTime();
		DataSet ds01 = ds.select("local_city", "imsi", "msisdn");
		System.out.println("select used: " + String.format("%.2fms", (System.nanoTime() - s) / 1000000));
		System.out.println("select result size:" + ds01.size());
		System.out.println("select result:" + ds01);

		
		s = System.nanoTime();
		DataSet ds02 = ds01.distinct();

		System.out.println("distinct used: " + String.format("%.2fms", (System.nanoTime() - s) / 1000000));
		System.out.println("distinct result size:" + ds02.size());
		System.out.println("distinct result:" + ds02);

		//System.out.println(ds.distinct("local_city"));
		System.out.println("test completed");
		
	}
	
	
	public static Schema getHttpSchema() {
		Schema schema = Schema.createStruct("s1u_http");
		schema.addField(Schema.create("length", "int"));
		schema.addField(Schema.create("local_province", "smallint"));
		schema.addField(Schema.create("local_city", "smallint"));
		schema.addField(Schema.create("owner_province", "smallint"));
		schema.addField(Schema.create("owner_city", "smallint"));
		schema.addField(Schema.create("roaming_type", "smallint"));
		schema.addField(Schema.create("interface", "smallint"));
		schema.addField(Schema.create("xdrid", "string"));
		schema.addField(Schema.create("rat", "smallint"));
		schema.addField(Schema.create("imsi", "string"));
		schema.addField(Schema.create("imei", "string"));
		schema.addField(Schema.create("msisdn", "string"));
		schema.addField(Schema.create("iptype", "smallint"));
		schema.addField(Schema.create("sgw_or_ggsn_ip", "string"));
		schema.addField(Schema.create("enb_or_sgsn_ip", "string"));
		schema.addField(Schema.create("pgw_add", "string"));
		schema.addField(Schema.create("sgw_or_ggsn_port", "int"));
		schema.addField(Schema.create("enb_or_sgsn_port", "int"));
		schema.addField(Schema.create("pgw_port", "int"));
		schema.addField(Schema.create("enb_or_sgsn_u_teid", "bigint"));
		schema.addField(Schema.create("sgw_or_ggsn_u_teid", "bigint"));
		schema.addField(Schema.create("tac", "int"));
		schema.addField(Schema.create("cellid", "bigint"));
		schema.addField(Schema.create("apn", "string"));
		schema.addField(Schema.create("apptype_code", "smallint"));
		schema.addField(Schema.create("startdate", "bigint"));
		schema.addField(Schema.create("enddate", "bigint"));
		schema.addField(Schema.create("lon", "string"));
		schema.addField(Schema.create("lat", "string"));
		schema.addField(Schema.create("protocoltype", "int"));
		schema.addField(Schema.create("apptype", "int"));
		schema.addField(Schema.create("appsubtype", "int"));
		schema.addField(Schema.create("appcontent", "smallint"));
		schema.addField(Schema.create("appstatus", "smallint"));
		schema.addField(Schema.create("ip_address_type", "string"));
		schema.addField(Schema.create("useripv4", "string"));
		schema.addField(Schema.create("useripv6", "string"));
		schema.addField(Schema.create("userport", "int"));
		schema.addField(Schema.create("l4_protocal", "smallint"));
		schema.addField(Schema.create("appserver_ipv4", "string"));
		schema.addField(Schema.create("appserver_ipv6", "string"));
		schema.addField(Schema.create("appserver_port", "int"));
		schema.addField(Schema.create("ulthroughput", "bigint"));
		schema.addField(Schema.create("dlthroughput", "bigint"));
		schema.addField(Schema.create("ulpackets", "bigint"));
		schema.addField(Schema.create("dlpackets", "bigint"));
		schema.addField(Schema.create("updura", "bigint"));
		schema.addField(Schema.create("downdura", "bigint"));
		schema.addField(Schema.create("ultcp_disorder_packets", "bigint"));
		schema.addField(Schema.create("dltcp_disorder_packets", "bigint"));
		schema.addField(Schema.create("ultcp_retransfer_packets", "bigint"));
		schema.addField(Schema.create("dltcp_retransfer_packets", "bigint"));
		schema.addField(Schema.create("ultcp_responsetime", "bigint"));
		schema.addField(Schema.create("dltcp_responsetime", "bigint"));
		schema.addField(Schema.create("ultcp_flag_packets", "bigint"));
		schema.addField(Schema.create("dltcp_flag_packets", "bigint"));
		schema.addField(Schema.create("tcplink_responsetime1", "bigint"));
		schema.addField(Schema.create("tcplink_responsetime2", "bigint"));
		schema.addField(Schema.create("windowsize", "bigint"));
		schema.addField(Schema.create("msssize", "bigint"));
		schema.addField(Schema.create("tcplink_count", "smallint"));
		schema.addField(Schema.create("tcplink_state", "smallint"));
		schema.addField(Schema.create("session_end", "smallint"));
		schema.addField(Schema.create("tcp_syn_ack_mum", "smallint"));
		schema.addField(Schema.create("tcp_ack_num", "smallint"));
		schema.addField(Schema.create("tcp12__status", "smallint"));
		schema.addField(Schema.create("tcp23__status", "smallint"));
		schema.addField(Schema.create("repetition", "smallint"));
		schema.addField(Schema.create("version", "smallint"));
		schema.addField(Schema.create("transtype", "smallint"));
		schema.addField(Schema.create("code", "smallint"));
		schema.addField(Schema.create("responsetime", "bigint"));
		schema.addField(Schema.create("lastpacket_delay", "bigint"));
		schema.addField(Schema.create("lastpacketack_delay", "bigint"));
		schema.addField(Schema.create("host", "string"));
		schema.addField(Schema.create("urilength", "int"));
		schema.addField(Schema.create("uri", "string"));
		schema.addField(Schema.create("x_online_host", "string"));
		schema.addField(Schema.create("useragentlength", "int"));
		schema.addField(Schema.create("useragent", "string"));
		schema.addField(Schema.create("content_type", "string"));
		schema.addField(Schema.create("refer_urilength", "int"));
		schema.addField(Schema.create("refer_uri", "string"));
		schema.addField(Schema.create("cookielength", "int"));
		schema.addField(Schema.create("cookie", "string"));
		schema.addField(Schema.create("contentlength", "int"));
		schema.addField(Schema.create("keyword", "string"));
		schema.addField(Schema.create("action", "int"));
		schema.addField(Schema.create("finish", "smallint"));
		schema.addField(Schema.create("delay", "int"));
		schema.addField(Schema.create("browse_tool", "int"));
		schema.addField(Schema.create("portals", "int"));
		schema.addField(Schema.create("locationlength", "int"));
		schema.addField(Schema.create("location", "string"));
		schema.addField(Schema.create("firstrequest", "smallint"));
		schema.addField(Schema.create("useraccount", "bigint"));
		return schema;
	}

}
