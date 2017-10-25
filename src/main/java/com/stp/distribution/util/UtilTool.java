package com.stp.distribution.util;

import java.net.InetAddress;

public class UtilTool {
	public static String getLocalIp() {

		String localIp = "";

		try {

			InetAddress inet = InetAddress.getLocalHost();

			localIp = inet.getHostAddress();

			return localIp;

		} catch (Exception e) {

			e.printStackTrace();

		}

		return null;
	}

}
