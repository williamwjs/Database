package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "yjtang.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}

    public static void closeEverything(ResultSet rs, Statement stmt) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) { }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) { }
        }
        /*if (con != null) {
            try {
                con.close();
            } catch (SQLException e) { }
        }*/
    }

	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() throws SQLException{ 
		
		// Scrollable result set allows us to read forward (using next())
		// and also backward.  
		// This is needed here to support the user of isFirst() and isLast() methods,
		// but in many cases you will not need it.
		// To create a "normal" (unscrollable) statement, you would simply call
		// Statement stmt = oracleConnection.createStatement();
		//
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		        ResultSet.CONCUR_READ_ONLY);
		
		// For each month, find the number of friends born that month
		// Sort them in descending order of count
		ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from "+
				userTableName+
				" where month_of_birth is not null group by month_of_birth order by 1 desc");
		
		this.monthOfMostFriend = 0;
		this.monthOfLeastFriend = 0;
		this.totalFriendsWithMonthOfBirth = 0;
		
		// Get the month with most friends, and the month with least friends.
		// (Notice that this only considers months for which the number of friends is > 0)
		// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
		//
		while(rst.next()) {
			int count = rst.getInt(1);
			int month = rst.getInt(2);
			if (rst.isFirst())
				this.monthOfMostFriend = month;
			if (rst.isLast())
				this.monthOfLeastFriend = month;
			this.totalFriendsWithMonthOfBirth += count;
		}
		
		// Get the names of friends born in the "most" month
		rst = stmt.executeQuery("select user_id, first_name, last_name from "+
				userTableName+" where month_of_birth="+this.monthOfMostFriend);
		while(rst.next()) {
			Long uid = rst.getLong(1);
			String firstName = rst.getString(2);
			String lastName = rst.getString(3);
			this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Get the names of friends born in the "least" month
		rst = stmt.executeQuery("select first_name, last_name, user_id from "+
				userTableName+" where month_of_birth="+this.monthOfLeastFriend);
		while(rst.next()){
			String firstName = rst.getString(1);
			String lastName = rst.getString(2);
			Long uid = rst.getLong(3);
			this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException { // Query1
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        //------------longest last time------------------
        /**************************************************************************************
         select distinct LAST_NAME from yjtang.public_USERS
         where length( LAST_NAME ) =
         ( select MAX(length( LAST_NAME )) from yjtang.public_USERS );
         */
        ResultSet rst = stmt.executeQuery("select distinct LAST_NAME from " + userTableName +
                " where length( LAST_NAME ) = " +
                "(select MAX(length( LAST_NAME )) from " + userTableName+")");
        /*************************************************************************************/

        try {
            while (rst.next()) {
                this.longestLastNames.add(rst.getString(1));
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst.close();

        // ------------shortest last time------------------
        /**************************************************************************************
         select distinct LAST_NAME from yjtang.public_USERS
         where length( LAST_NAME ) =
         ( select MIN(length( LAST_NAME )) from yjtang.public_USERS );
        */
        ResultSet rst1 = stmt.executeQuery("select distinct LAST_NAME from " + userTableName +
                " where length( LAST_NAME ) = " +
                "(select MIN(length( LAST_NAME )) from " + userTableName+")");
        /*************************************************************************************/

        try {
            while (rst1.next()) {
                this.shortestLastNames.add(rst1.getString(1));
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst1, stmt);
        }

        rst1.close();

        // ------------most common last name------------------
        /**************************************************************************************
         create view CountNum as
         select LAST_NAME, count(*) as num from yjtang.public_USERS
         group by LAST_NAME;
        */
        ResultSet rst3 = stmt.executeQuery("create or replace view CountNum as select LAST_NAME, "
                + "count(*) as num from " + userTableName +" group by LAST_NAME");
        /*************************************************************************************/

        /**************************************************************************************
         select LAST_NAME from CountNum, (select MAX(num) as N from CountNum) A
         where num = A.N;
        */
        ResultSet rst4 = stmt.executeQuery("select LAST_NAME,num from CountNum, (select MAX(num) as N "
                + "from CountNum) A where num = A.N");
        /*************************************************************************************/

        try {
            while (rst4.next()) {
                this.mostCommonLastNames.add(rst4.getString(1));
                this.mostCommonLastNamesCount = rst4.getInt(2);
            }
            ResultSet rst5 = stmt.executeQuery("Drop view CountNum");
            rst5.close();
        } catch (SQLException e) { // print out an error message.
            rst4.close();
            closeEverything(rst3, stmt);
        }

        rst4.close();
        rst3.close();
        stmt.close();
	}
	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have no friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void lonelyFriends() throws SQLException {
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        /**************************************************************************************
         SELECT U.user_id, U.first_name, U.last_name
         FROM yjtang.PUBLIC_USERS U
         MINUS
         SELECT U.user_id, U.first_name, U.last_name
         FROM yjtang.PUBLIC_USERS U, yjtang.PUBLIC_FRIENDS F
         WHERE U.user_id = F.user1_id OR U.user_id = F.user2_id;
        */
        ResultSet rst = stmt.executeQuery("SELECT U.user_id, U.first_name, U.last_name " +
                "FROM " + userTableName + " U " +
                "MINUS " +
                "SELECT U.user_id, U.first_name, U.last_name " +
                "FROM " + userTableName + " U, " + friendsTableName + " F " +
                "WHERE U.user_id = F.user1_id OR U.user_id = F.user2_id");
        /*************************************************************************************/

        try {
            int count = 0;

            while (rst.next()) {
                this.lonelyFriends.add(new UserInfo(rst.getLong(1), rst.getString(2),
                        rst.getString(3)));
                count++;
            }
            this.countLonelyFriends = count;
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst.close();
        stmt.close();
	}
	 

	@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
		this.liveAtHome.add(new UserInfo(11L, "Heather", "Hometowngirl"));
		this.countLiveAtHome = 1;
	}



	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException {
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
        /**************************************************************************************
         SELECT P.photo_id, P.album_id, A.album_name, P.photo_caption, P.photo_link, U.user_id, U.first_name, U.last_name, C.tag_count
         FROM yjtang.PUBLIC_PHOTOS P, yjtang.PUBLIC_ALBUMS A, yjtang.PUBLIC_USERS U, yjtang.PUBLIC_TAGS T,
         ( SELECT P.photo_id, B.tag_count
           FROM yjtang.PUBLIC_PHOTOS P,
           ( SELECT tag_photo_id, COUNT(*) AS tag_count
             FROM yjtang.PUBLIC_TAGS
             GROUP BY tag_photo_id
             ORDER BY tag_count DESC, tag_photo_id ASC) B
           WHERE ROWNUM <= 5 AND B.tag_photo_id = P.photo_id) C
         WHERE C.photo_id = P.photo_id AND P.album_id = A.album_id AND T.tag_photo_id = P.photo_id AND U.user_id = T.tag_subject_id;
         */
        ResultSet rst = stmt.executeQuery("SELECT P.photo_id, P.album_id, " +
                "A.album_name, P.photo_caption, P.photo_link, U.user_id, " +
                "U.first_name, U.last_name, C.tag_count\n" +
                "FROM " + photoTableName + " P, " + albumTableName +
                " A, " + userTableName + " U, " + tagTableName + " T,\n" +
                "(SELECT P.photo_id, B.tag_count\n" +
                "FROM " + photoTableName + " P, \n" +
                "(SELECT tag_photo_id, COUNT(*) AS tag_count\n" +
                "FROM " + tagTableName + "\n" +
                "GROUP BY tag_photo_id\n" +
                "ORDER BY tag_count DESC, tag_photo_id ASC) B\n" +
                "WHERE ROWNUM <= " + n + " AND B.tag_photo_id = P.photo_id) C\n" +
                "WHERE C.photo_id = P.photo_id AND P.album_id = A.album_id " +
                "AND T.tag_photo_id = P.photo_id AND U.user_id = T.tag_subject_id");

        try {
            while (rst.next()) {
                PhotoInfo p = new PhotoInfo(rst.getString(1), rst.getString(2),
                        rst.getString(3), rst.getString(4), rst.getString(5));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(new UserInfo(rst.getLong(6), rst.getString(7), rst.getString(8)));
                for (int i = 0; i < rst.getInt(9)-1; i++) {
                    rst.next();
                    tp.addTaggedUser(new UserInfo(rst.getLong(6), rst.getString(7), rst.getString(8)));
                }
                this.photosWithMostTags.add(tp);
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst.close();
        stmt.close();
	}

	
	
	
	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller user_id for the female
	// (iii) If there are still ties, choose the pair with the smaller user_id for the male
	//
	public void matchMaker(int n, int yearDiff) throws SQLException { 
		Long girlUserId = 123L;
		String girlFirstName = "girlFirstName";
		String girlLastName = "girlLastName";
		int girlYear = 1988;
		Long boyUserId = 456L;
		String boyFirstName = "boyFirstName";
		String boyLastName = "boyLastName";
		int boyYear = 1986;
		MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
				girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
		String sharedPhotoId = "12345678";
		String sharedPhotoAlbumId = "123456789";
		String sharedPhotoAlbumName = "albumName";
		String sharedPhotoCaption = "caption";
		String sharedPhotoLink = "link";
		mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
				sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
		this.bestMatches.add(mp);
	}

	
	
	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n) throws SQLException {
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        /**************************************************************************************
         CREATE OR REPLACE VIEW getfriend AS
         SELECT U.user_id, F.user2_id AS friend_id
         FROM yjtang.PUBLIC_USERS U, yjtang.PUBLIC_FRIENDS F
         WHERE U.user_id = F.user1_id
         UNION
         SELECT U.user_id, F.user1_id AS friend_id
         FROM yjtang.PUBLIC_USERS U, yjtang.PUBLIC_FRIENDS F
         WHERE U.user_id = F.user2_id;
        */
        ResultSet rst = stmt.executeQuery("CREATE OR REPLACE VIEW getfriend AS\n" +
                "SELECT U.user_id, F.user2_id AS friend_id\n" +
                "FROM " + userTableName + " U, " + friendsTableName + " F\n" +
                "WHERE U.user_id = F.user1_id\n" +
                "UNION\n" +
                "SELECT U.user_id, F.user1_id AS friend_id\n" +
                "FROM " + userTableName + " U, " + friendsTableName + " F\n" +
                "WHERE U.user_id = F.user2_id");
        /*************************************************************************************/

        /**************************************************************************************
         CREATE OR REPLACE VIEW totalfriend AS
         SELECT G1.user_id AS user1_id, G2.user_id AS user2_id, G1.friend_id
         FROM getfriend G1, getfriend G2
         WHERE G1.friend_id = G2.friend_id AND G1.user_id < G2.user_id;
        */
        rst = stmt.executeQuery("CREATE OR REPLACE VIEW totalfriend AS\n" +
                "SELECT G1.user_id AS user1_id, G2.user_id AS user2_id, G1.friend_id\n" +
                "FROM getfriend G1, getfriend G2\n" +
                "WHERE G1.friend_id = G2.friend_id AND G1.user_id < G2.user_id");
        /*************************************************************************************/

        /**************************************************************************************
         CREATE OR REPLACE VIEW sharefriend AS
         SELECT T.user1_id, T.user2_id, T.friend_id
         FROM totalfriend T,
         ( SELECT user1_id, user2_id
           FROM totalfriend
           MINUS
           SELECT user1_id, user2_id
           FROM yjtang.PUBLIC_FRIENDS) D
         WHERE T.user1_id = D.user1_id AND T.user2_id = D.user2_id;
        */
        rst = stmt.executeQuery("CREATE OR REPLACE VIEW sharefriend AS\n" +
                "SELECT T.user1_id, T.user2_id, T.friend_id\n" +
                "FROM totalfriend T,\n" +
                "(SELECT user1_id, user2_id\n" +
                "FROM totalfriend\n" +
                "MINUS\n" +
                "SELECT user1_id, user2_id\n" +
                "FROM " + friendsTableName + ") D\n" +
                "WHERE T.user1_id = D.user1_id AND T.user2_id = D.user2_id");
        /*************************************************************************************/

        /**************************************************************************************
         SELECT S.user1_id, U1.first_name, U1.last_name, S.user2_id, U2.first_name, U2.last_name, S.friend_id, U3.first_name, U3.last_name, C.countshare
         FROM sharefriend S, yjtang.PUBLIC_USERS U1, yjtang.PUBLIC_USERS U2, yjtang.PUBLIC_USERS U3,
         ( SELECT user1_id, user2_id, countshare
           FROM
           (  SELECT user1_id, user2_id, COUNT(*) AS countshare
              FROM sharefriend
              GROUP BY user1_id, user2_id
              ORDER BY countshare DESC, user1_id ASC, user2_id ASC)
           WHERE ROWNUM <= 2
           ) C
         WHERE C.user1_id = S.user1_id AND C.user2_id = S.user2_id AND U1.user_id = S.user1_id AND U2.user_id = S.user2_id AND U3.user_id = S.friend_id;
        */
        rst = stmt.executeQuery("SELECT S.user1_id, U1.first_name, U1.last_name, " +
                "S.user2_id, U2.first_name, U2.last_name, " +
                "S.friend_id, U3.first_name, U3.last_name, C.countshare\n" +
                "FROM sharefriend S, " + userTableName + " U1, " + userTableName +
                " U2, " + userTableName + " U3, \n" +
                "(SELECT user1_id, user2_id, countshare\n" +
                "FROM\n" +
                "(SELECT user1_id, user2_id, COUNT(*) AS countshare\n" +
                "FROM sharefriend\n" +
                "GROUP BY user1_id, user2_id\n" +
                "ORDER BY countshare DESC, user1_id ASC, user2_id ASC)\n" +
                "WHERE ROWNUM <= 2\n" +
                ") C\n" +
                "WHERE C.user1_id = S.user1_id AND C.user2_id = S.user2_id " +
                "AND U1.user_id = S.user1_id AND U2.user_id = S.user2_id " +
                "AND U3.user_id = S.friend_id");
        /*************************************************************************************/

        try {
            while (rst.next()) {
                FriendsPair p = new FriendsPair(rst.getLong(1), rst.getString(2), rst.getString(3),
                        rst.getLong(4), rst.getString(5), rst.getString(6));
                p.addSharedFriend(rst.getLong(7), rst.getString(8), rst.getString(9));
                for (int i = 0; i < rst.getInt(10)-1; i++) {
                    rst.next();
                    p.addSharedFriend(rst.getLong(7), rst.getString(8), rst.getString(9));
                }
                this.suggestedFriendsPairs.add(p);
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst = stmt.executeQuery("DROP VIEW sharefriend");
        rst = stmt.executeQuery("DROP VIEW totalfriend");
        rst = stmt.executeQuery("DROP VIEW getfriend");

        rst.close();
        stmt.close();
	}
	
	
	//@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) throws SQLException {
		this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
		this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
	}
	
	
	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        /**************************************************************************************
         CREATE OR REPLACE VIEW numcity AS
         SELECT event_city_id, COUNT(*) AS city_events
         FROM yjtang.PUBLIC_USER_EVENTS
         GROUP BY event_city_id;
        */
        ResultSet rst = stmt.executeQuery("CREATE OR REPLACE VIEW numcity AS\n" +
                "SELECT event_city_id, COUNT(*) AS city_events\n" +
                "FROM " + eventTableName + "\n" +
                "GROUP BY event_city_id");
        /*************************************************************************************/

        /**************************************************************************************
         SELECT C.city_name, N.city_events
         FROM yjtang.PUBLIC_CITIES C, numcity N,
         ( SELECT MAX(city_events) AS maxcity FROM numcity) M
         WHERE N.city_events = M. maxcity AND N.event_city_id = C.city_id;
        */
        rst = stmt.executeQuery("SELECT C.city_name, N.city_events\n" +
                "FROM " + cityTableName + " C, numcity N,\n" +
                "(SELECT MAX(city_events) AS maxcity FROM numcity) M\n" +
                "WHERE N.city_events = M. maxcity AND N.event_city_id = C.city_id");
        /*************************************************************************************/

        try {
            while (rst.next()) {
                this.popularCityNames.add(rst.getString(1));
                this.eventCount = rst.getInt(2);
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst = stmt.executeQuery("DROP VIEW numcity");

        rst.close();
        stmt.close();
	}
	
	
	
	@Override
//	 ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
	// on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
	//  
	//
	public void findPotentialSiblings() throws SQLException {
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
		this.siblings.add(s);
	}
	
}
