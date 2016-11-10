/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author mayur
 */
public class Populate {
    public static void main(String[] argv) {
		CreateUser c = new CreateUser();               
                CreateReview r = new CreateReview();
                Createcheckin c1 = new Createcheckin();
                CreateBusiness b = new CreateBusiness();
                        
                System.out.println("Records successfully added!! ");
	}

}


class CreateUser{

 CreateUser()
    {
    try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

		} catch (ClassNotFoundException e) {

			System.out.println("JDBC Driver Missing");
			e.printStackTrace();
			return;

		}

		System.out.println("Oracle JDBC Driver Connected");

		Connection conn = null;

		try {

			conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "MAYUR","123456");                        
                        conn.setAutoCommit(false);

PreparedStatement ps = conn.prepareStatement("insert into yelp_user values (?, ?, ?,?, ?, ?,?, ?, ?,?,?)"); 
     
JSONParser parser = new JSONParser();
Object obj = parser.parse(new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\yelp_user.json")));

//Object obj = parser.parse(new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\yelp_user1.json")));

JSONArray jsonArray = (JSONArray)(obj);

      // JSONArray jsonArray = (JSONArray)(obj1);
for (int i = 0; i < jsonArray.size(); i++)
{
    JSONObject t = (JSONObject)jsonArray.get(i);
    
 

    String c = t.get("yelping_since").toString();
    
    Date yelping_since = (Date)java.sql.Date.valueOf(c+"-01"); 
    
    JSONObject votes = (JSONObject)t.get("votes");      // get all votes details
    Long votes_funny = (Long)votes.get("funny");
    Long votes_useful = (Long)votes.get("useful");    
    Long votes_cool = (Long)votes.get("cool");

       
    Long review_count = (Long)t.get("review_count");   
    String name = t.get("name").toString();
    String user_id = t.get("user_id").toString();
    
    JSONArray friends = (JSONArray)(t).get("friends");
    int numfriends=0;
    if(friends!=null)
    { 
    Iterator<String> iterator = friends.iterator();
    ArrayList<String> friendid_list = new ArrayList<String>(); 
    
            while (iterator.hasNext())
                {
                    friendid_list.add(iterator.next());                 
                }
            
            if(friendid_list!=null)
            numfriends = friendid_list.size();
            
            friendid_list =null;
            iterator = null;
            
    }       
    Long fans = (Long)t.get("fans");   
    double average_stars = (double)t.get("average_stars");   
    String type = t.get("type").toString();
   
    
    ps.setDate(1, yelping_since); 
    ps.setLong(2,votes_funny); 
    ps.setLong(3, votes_useful); 
    ps.setLong(4, votes_cool); 
    ps.setLong(5,review_count); 
    ps.setString(6, name);
    ps.setString(7, user_id); 
    ps.setLong(8,fans); 
    ps.setDouble(9, average_stars);
    ps.setString(10, type);
    ps.setInt(11, numfriends);
    
    ps.executeUpdate(); 
    System.out.println("Record inserted "+ i);
   
}
 
conn.commit();

ps.close();

		} 
                catch (Exception e) {

			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;

		}

    }
}

class CreateReview{


    CreateReview() {

        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            System.out.println("JDBC Driver Missing");
            e.printStackTrace();
            return;

        }

        System.out.println("Oracle JDBC Driver Connected");

        Connection conn = null;
        PreparedStatement ps=null;
        try {

            conn = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:xe", "MAYUR", "123456");

           
            
            JSONParser parser = new JSONParser();
            BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\yelp_review.json"));
           // BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\reviewshortfile.json"));
          
            int batch = 250;    
            int count = 0;
            String sCurrentLine = "";
            String temp="";
            while ((sCurrentLine = br.readLine()) != null) {

            conn.setAutoCommit(false);
     
                Object obj = parser.parse(sCurrentLine);

                JSONObject t = (JSONObject) obj;

                JSONObject votes = (JSONObject) t.get("votes");      // get all votes details
                Long votes_funny = (Long) votes.get("funny");
                Long votes_useful = (Long) votes.get("useful");
                Long votes_cool = (Long) votes.get("cool");
                String user_id = t.get("user_id").toString();
                String review_id = t.get("review_id").toString();
                Long stars = (Long) t.get("stars");
                String c = t.get("date").toString();
                Date review_date = (Date) java.sql.Date.valueOf(c);

                String text = t.get("text").toString();
                if (text.length() > 1600) {
                    temp = text.substring(0, 1600);
                    text = "";
                    text = temp;
                    System.out.println("String truncated "+text.length()+"at record"+count);
                    temp="";
                }
                String type = t.get("type").toString();
                String business_id = t.get("business_id").toString();

                ps = conn.prepareStatement("insert into review values (?, ?, ?,?, ?, ?,?, ?, ?,?)");

                ps.setLong(1, votes_funny);
                ps.setLong(2, votes_useful);
                ps.setLong(3, votes_cool);
                ps.setString(4, user_id);
                ps.setString(5, review_id);
                ps.setLong(6, stars);
                ps.setDate(7, review_date);

                ps.setString(8, text);
                
                ps.setString(9, type);
                ps.setString(10, business_id);

               
               ps.executeUpdate();
               count++;
               System.out.println("Record Number :"+ count);  
               if(count%batch==0){
               conn.commit();
               ps.close();
               conn.close();
               conn = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:xe", "MAYUR", "123456");

               }
            

            }

            System.out.println("Record inserted Finally in Review Table " + count);
           conn.commit();
            ps.close();
            conn.close();
            

        } catch (Exception e) {
            
            
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
            
            

        }
        finally{
       try{ 
           ps.close();
           conn.close();
          }
        catch(Exception e){
        
        }
        }

    }
}

class Createcheckin{

Createcheckin() {

        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            System.out.println("JDBC Driver Missing");
            e.printStackTrace();
            return;

        }

        System.out.println("Oracle JDBC Driver Connected");

        Connection conn = null;
        PreparedStatement ps=null;
        try {

            conn = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:xe", "MAYUR", "123456");

           
            
            JSONParser parser = new JSONParser();
            BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\yelp_checkin.json"));
            //BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\checkinsmall.json"));
          
            int batch = 50;    
            int count = 0;
            String sCurrentLine = "";
            String temp="";
            while ((sCurrentLine = br.readLine()) != null) {

            conn.setAutoCommit(false);
     
                Object obj = parser.parse(sCurrentLine);
                JSONObject t = (JSONObject) obj;
                String type = t.get("type").toString();
                String business_id = t.get("business_id").toString();
                JSONObject checkinInfo = (JSONObject) t.get("checkin_info");      // get all votes details
                
                ps = conn.prepareStatement("insert into checkin values (?, ?, ?,?,?)");
                
                ps.setString(4, business_id);
                ps.setString(5, type);                             
                for(int i=0;i<7;i++)
                {
                    for(int j=0;j<24;j++)
                    {
                        String temp_key=j+"-"+i;
                        if(checkinInfo.containsKey(temp_key))
                        {
                            long num_checkin = (long)checkinInfo.get(temp_key);
                            //i - day
                            //j - time
                            //num checkin - number
                            //Insert
                             ps.setLong(1, (long)j); // hours
                             ps.setLong(2, (long)i); //day
                             ps.setLong(3,num_checkin );                          
                             ps.executeUpdate();
                            
                        }
                    }
                }

                
               
               
               count++;
               System.out.println("Record Number :"+ count);  
               if(count%batch==0){
               conn.commit();
               ps.close();
               conn.close();
               conn = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:xe", "MAYUR", "123456");

               }
            

            }

            System.out.println("Record inserted Finally in Checkin Table " + count);
           conn.commit();
            ps.close();
            conn.close();
            

        } catch (Exception e) {
            
            
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
            
            

        }
        finally{
       try{ 
           ps.close();
           conn.close();
          }
        catch(Exception e){
        
        }
        }

    }
}

class CreateBusiness{
    
     CreateBusiness()
    {
        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            System.out.println("JDBC Driver Missing");
            e.printStackTrace();
            return;

        }

        System.out.println("Oracle JDBC Driver Connected");

        Connection conn = null;
        PreparedStatement ps=null;
        PreparedStatement psbusinesscat=null; 
        PreparedStatement pscate=null;
        try {

            conn = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:xe", "MAYUR", "123456");

           
            
            JSONParser parser = new JSONParser();
            BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\yelp_business.json"));
            // BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\mayur\\Downloads\\YelpDataset\\YelpDataset-CptS451\\smallbusiness.json"));
            
            int batch = 200;    
            int count = 0;
            String sCurrentLine = "";
            String temp="";
            ArrayList<String> cat = new ArrayList<String>();
                cat.add("Active Life");
                cat.add("Arts & Entertainment");
                cat.add("Automotive");
                cat.add("Car Rental");
                cat.add("Cafes");
                cat.add("Beauty & Spas");
                cat.add("Convenience Stores");
                cat.add("Dentists");
                cat.add("Doctors");
                cat.add("Drugstores");
                cat.add("Department Stores");
                 cat.add("Education");
                cat.add("Event Planning & Services");
                 cat.add("Flower & Gifts");                
                 cat.add("Food");
                 cat.add("Health & Medical");
                 cat.add("Home Services");
                 cat.add("Home & Gardens");
                 cat.add("Hospitals");
                 cat.add("Hotels & Travel");
                 cat.add("Hardware Stores");
                 cat.add("Grocery");
                 cat.add("Medical Centers");
                 cat.add("Nurseries & Gardening");
                 cat.add("Nightlife");
                 cat.add("Shopping");
                 cat.add("Restaurants");
                 cat.add("Transportation");
            
            while ((sCurrentLine = br.readLine()) != null) {

            conn.setAutoCommit(false);
     
                Object obj = parser.parse(sCurrentLine);

                JSONObject t = (JSONObject) obj;

                String business_id = t.get("business_id").toString();
                String city = t.get("city").toString();
                String state = t.get("state").toString();
                Long review_count = (Long) t.get("review_count");
                Double stars = (Double) t.get("stars");
                Double latitude = (Double) t.get("latitude");
                Double longitude = (Double) t.get("longitude");
               
                String business_name = t.get("name").toString();

                String type = t.get("type").toString();
                
                ps = conn.prepareStatement("insert into business values (?, ?, ?,?,?,?, ?, ?,?)");
                
                ps.setString(1, business_id);    
                ps.setString(2, city);
                ps.setString(3, state);
                ps.setLong(4, review_count);
                ps.setDouble(5, stars);
                ps.setDouble(6, latitude);
                ps.setDouble(7, longitude);
                ps.setString(8, business_name);
                ps.setString(9, type);
                ps.executeUpdate();
                ps.close();
                conn.commit();
                
                psbusinesscat = conn.prepareStatement("insert into categories values (?,?,?)");
                
               
               JSONArray categories = (JSONArray)(t).get("categories");
               int numfriends=0;
               if( categories.size()>0 )            
                { 
                ArrayList<String> categories_list = new ArrayList<String>(); 
                ArrayList<String> subcategories_list = new ArrayList<String>(); 
                ArrayList<String> list = new ArrayList<String>();
               // Retrive categories from json and seperate it as category and subcategory 
               for (int i = 0; i < categories.size(); i++)
                {
                    String tempCat = categories.get(i).toString();
                    if(cat.contains(tempCat))
                       categories_list.add(tempCat);
                    else
                        subcategories_list.add(tempCat);
                    
                }
               
               psbusinesscat.setString(1, business_id);
               for(int i =0;i < categories_list.size();i++)
               {
               System.out.println(categories_list.get(i)+" ");
                    psbusinesscat.setString(2, categories_list.get(i));
                   if(subcategories_list.size()>0)
                   {    
                         // set category for business table   
                              
                        for(int j = 0; j<subcategories_list.size();j++)
                          {
                                System.out.println("-"+ subcategories_list.get(j));
                                psbusinesscat.setString(3, subcategories_list.get(j));
                                psbusinesscat.executeUpdate();
                                
                                list.add(categories_list.get(i)+"~"+subcategories_list.get(j));
                                
                                //insert into business categories list
                          }
                        
                   }
                   else{

                   psbusinesscat.setString(3, "");
                   psbusinesscat.executeUpdate();
                   
                   System.out.println("");
                   
                   }
                   
               }
               psbusinesscat.close();
               conn.commit();
               
               List<String> deDupStringList = new ArrayList<>(new HashSet<>(list));

               pscate = conn.prepareStatement("insert into categoryclass values (?,?)");
               
               
                for(int i =0;i < deDupStringList.size();i++)
               {
                    
                  pscate.setString(1, deDupStringList.get(i).substring(0, deDupStringList.get(i).indexOf("~")));
                  System.out.println(deDupStringList.get(i).indexOf("~"));
                  pscate.setString(2, deDupStringList.get(i).substring( deDupStringList.get(i).indexOf("~")+1));
                         
                  pscate.executeUpdate(); 
                        
                }   
                            
               pscate.close();
              }
               conn.commit();               
                
               count++;
               System.out.println("Record Number :"+ count);  
               if(count%50==0){                         //reload driver               
               conn.close();

               conn = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:xe", "MAYUR", "123456");

               }
            

            }

            System.out.println("Last record inserted is " + count);
           conn.commit();
            ps.close();
            conn.close();
            

        } catch (Exception e) {
            
            
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
            
            

        }
        finally{
       try{ 
           ps.close();
           pscate.close();
           psbusinesscat.close();
           conn.close();
          }
        catch(Exception e){
        
        }
        }
    }
}