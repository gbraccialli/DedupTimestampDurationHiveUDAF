package com.github.gbraccialli.test;

import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.text.BreakIterator;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class Test {
	
	private static Driver phoenixDriver;
	private static Connection conn = null;
	
	public static void main(String[] args) throws IOException {


		System.out.println("11984808108".hashCode() % 11);
		System.out.println(simplePhoneNumberHash("11984808108",20));
		System.out.println(simplePhoneNumberHash("11981341618",20));
		System.out.println(simplePhoneNumberHash("11982012336",20));
		System.out.println(simplePhoneNumberHash("11999184262",20));
		System.out.println(simplePhoneNumberHash("11999839553",20));
		System.out.println(simplePhoneNumberHash("11972032146",20));
		System.out.println(simplePhoneNumberHash("1136756919",20));
		System.out.println(simplePhoneNumberHash("1138683801",20));
		System.out.println(simplePhoneNumberHash("3599159819",20));
		System.out.println(simplePhoneNumberHash("11993279116",20));
		
		
		System.out.println("11994808108".hashCode() % 11);
		System.out.println("11981341618".hashCode() % 11);
		System.out.println("guilherme".hashCode() );

		
		
		//System.out.println(getPolarity("asdfasdas asdfa sdf asdf"));
		

	}
	
	private static void datesTests(){

		String created = "Sat, 12 Aug 1995 13:30:00 GMT";
		
		Date  d=new Date(created);
		//SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z+9HOUR'");
		//SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z-4HOUR'");
		SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		created = formatter.format(d);
		
		
		System.out.println("created" + created);
		
	}
	
	private static void userGroupTests() throws Exception{
		
		Process p = Runtime.getRuntime().exec("groups");
        
        BufferedReader stdInput = new BufferedReader(new
             InputStreamReader(p.getInputStream()));

        String group = stdInput.readLine();
        group = "test1 : group1";
		
		String groupsSplit[] = group.split(":");
		
		if (groupsSplit.length == 2)
			System.out.println(groupsSplit[1].trim().split(" ")[0].trim());
		else
			System.out.println("erro" + group);
	}
	
	private static int simplePhoneNumberHash(String phoneNumber, int buckets){
		
		int sum = 0;
		int i = 0;
		int factor[] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997};
		for (char number : phoneNumber.toCharArray()){
			sum += Byte.valueOf(String.valueOf(number)) * factor[i++];
		}
		return sum % buckets;
		
		
	}
	

	
	private static String getPolarity(String tweet){

		String polarity = "neutral";
		tweet = tweet.toLowerCase();
		BreakIterator wi = BreakIterator.getWordInstance();
		StringBuffer strBuffer = new StringBuffer();
		wi.setText(tweet);
		int widx = 0;
		while(wi.next() != BreakIterator.DONE) {
			String word = tweet.substring(widx, wi.current());
			widx = wi.current();
			if(Character.isLetterOrDigit(word.charAt(0))) {
				word = word.replaceAll("'", "\\\\'");
				strBuffer.append("word = '" +  word + "' OR ");
			}
		}

		
		try {
			//conn = phoenixDriver.connect("jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure",new Properties());	

			String query = "select case when positive > negative then 'positive' when negative > positive then 'negative' else 'neutral' end as polarity from (select coalesce(sum(case when polarity='positive' then 1 end),0) as positive, coalesce(sum(case when polarity='negative' then 1 end),0) as negative from dictionary where (" + strBuffer.substring(0, strBuffer.length()-4) + ")) t1;";
			System.out.println(query);
			ResultSet rst = conn.createStatement().executeQuery(query);
			
			while (rst.next()) {
				polarity = rst.getString(1);
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
			//ERROR SELECTION POLARITY IGNOR
			System.out.println("Error selection polarity:" + e.toString());

		}
		finally
		{
			try
			{
				if(conn != null)
					conn.close();
			}
			catch(Exception e)
			{
				throw new RuntimeException(e);
			}
				
		}
		
		return polarity;
	
	}

}
