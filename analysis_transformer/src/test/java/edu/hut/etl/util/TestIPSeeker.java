package edu.hut.etl.util;

import java.util.List;


public class TestIPSeeker {

	public static void main(String[] args) {
		IPSeekerExt seeker = new IPSeekerExt();
		List<String> ips = seeker.getAllIp();
		for (String ip: ips) {
			System.out.println(seeker.parseIp(ip));
		}
	} 

}
