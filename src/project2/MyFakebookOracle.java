package project2;

/**
 * Created by Jiashuo Wang and Jun Yin
 * Uniqname: willwjs, junyin
 */

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

        // ------------shortest last time------------------
        /**************************************************************************************
         select distinct LAST_NAME from yjtang.public_USERS
         where length( LAST_NAME ) =
         ( select MIN(length( LAST_NAME )) from yjtang.public_USERS );
        */
        rst = stmt.executeQuery("select distinct LAST_NAME from " + userTableName +
                " where length( LAST_NAME ) = " +
                "(select MIN(length( LAST_NAME )) from " + userTableName+")");
        /*************************************************************************************/

        try {
            while (rst.next()) {
                this.shortestLastNames.add(rst.getString(1));
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        // ------------most common last name------------------
        /**************************************************************************************
         create view CountNum as
         select LAST_NAME, count(*) as num from yjtang.public_USERS
         group by LAST_NAME;
        */
        rst = stmt.executeQuery("create or replace view CountNum as select LAST_NAME, "
                + "count(*) as num from " + userTableName +" group by LAST_NAME");
        /*************************************************************************************/

        /**************************************************************************************
         select LAST_NAME from CountNum, (select MAX(num) as N from CountNum) A
         where num = A.N;
        */
        rst = stmt.executeQuery("select LAST_NAME,num from CountNum, (select MAX(num) as N "
                + "from CountNum) A where num = A.N");
        /*************************************************************************************/

        try {
            while (rst.next()) {
                this.mostCommonLastNames.add(rst.getString(1));
                this.mostCommonLastNamesCount = rst.getInt(2);
            }
            rst = stmt.executeQuery("Drop view CountNum");
        } catch (SQLException e) { // print out an error message.
            closeEverything(rst, stmt);
        }

        rst.close();
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
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        /**************************************************************************************
         select A.user_id, A.first_name, A.LAST_NAME
         from yjtang.public_USERS A, yjtang.public_USER_CURRENT_CITY B, yjtang.public_USER_HOMETOWN_CITY C
         where A.user_id = B.user_id AND B.user_id = C.user_id AND B.current_city_id = C.HOMETOWN_CITY_ID;
        */
        ResultSet rst = stmt.executeQuery("select A.user_id, A.first_name, A.LAST_NAME from "
                + userTableName + " A," + currentCityTableName + " B," + hometownCityTableName + " C"
                + " where A.user_id = B.user_id AND B.user_id = C.user_id "
                + " AND B.current_city_id = C.HOMETOWN_CITY_ID ");
        /*************************************************************************************/

        this.countLiveAtHome = 0;
        try {
            while (rst.next()) {
                this.liveAtHome.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
                this.countLiveAtHome++;
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst.close();
        stmt.close();
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
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        /**************************************************************************************
         create or replace view Temp as
         select A.user_id as USER1_id, B.user_id as USER2_id
         from yjtang.public_USERS A, yjtang.public_USERS B
         WHERE A.gender = 'female' and B.gender = 'male'
         and abs(A.year_of_birth-B.year_of_birth) < 2
         MINUS
         select USER1_ID, USER2_id from yjtang.public_FRIENDS
         MINUS
         select USER2_id as USER1_ID, USER1_ID as USER2_id from yjtang.public_FRIENDS;
        */
        ResultSet rst = stmt.executeQuery(" create or replace view Temp as select A.user_id as user1_id, "
                + " B.user_id as user2_id from " + userTableName + " A, " + userTableName + " B "
                + " WHERE A.gender = 'female' and B.gender = 'male' "
                + " and abs(A.year_of_birth-B.year_of_birth) < " + yearDiff + " MINUS "
                + " select USER1_ID, USER2_id from " + friendsTableName + " MINUS "
                + " select USER2_id as USER1_ID, USER1_ID as USER2_id from " + friendsTableName);
        /*************************************************************************************/

        /**************************************************************************************
         create or replace view SharePhoto as
         select A.tag_subject_id as user1_id, B.tag_subject_id as user2_id,
         A.tag_photo_id as photo_id
         from yjtang.public_tags A, yjtang.public_tags B, Temp C
         where A.tag_photo_id = B.tag_photo_id
         and C.USER1_ID = A.tag_subject_id and C.USER2_id = B.tag_subject_id;
        */
        rst = stmt.executeQuery(" create or replace view SharePhoto as "
                + "select A.tag_subject_id as user1_id, B.tag_subject_id as user2_id, "
                + "A.tag_photo_id as photo_id from " + tagTableName + " A, "
                + tagTableName + " B, Temp C where A.tag_photo_id = B.tag_photo_id "
                + "and C.USER1_ID = A.tag_subject_id and C.USER2_id = B.tag_subject_id");
        /*************************************************************************************/

        /**************************************************************************************
         create or replace view PhotoCount as
         select user1_id, user2_id, count(*) as num from SharePhoto
         group by user1_id, user2_id
         order by num desc, user1_id asc, user2_id asc;
        */
        rst = stmt.executeQuery(" create or replace view PhotoCount as "
                + "select user1_id, user2_id, count(*) as num from SharePhoto "
                + "group by user1_id, user2_id "
                + "order by num desc, user1_id asc, user2_id asc");

        /**************************************************************************************
         select P.user1_id, A.first_name, A.last_name, A.year_of_birth,
         P.user2_id, B.first_name, B.last_name, B.year_of_birth,
         PH.photo_id, PH.album_id, AL.album_name, PH.photo_caption, PH.photo_link
         FROM PhotoCount P, SharePhoto S, yjtang.public_USERS A,
         yjtang.public_USERS B, yjtang.public_photos PH, yjtang.public_albums AL
         where P.user1_id = A.user_id AND P.user2_id = B.user_id
         AND P.user1_id = S.user1_id AND P.user2_id = S.user2_id
         AND S.photo_id = PH.photo_id AND PH.album_id = AL.album_id
         AND rownum < n;
        */
        rst = stmt.executeQuery(" select P.user1_id, A.first_name, A.last_name, A.year_of_birth,"
                + " P.user2_id, B.first_name, B.last_name, B.year_of_birth, PH.photo_id, "
                + "PH.album_id, AL.album_name, PH.photo_caption, PH.photo_link "
                + "FROM PhotoCount P, SharePhoto S, " + userTableName + " A,"
                + userTableName + " B," + photoTableName + " PH," + albumTableName + " AL "
                + "where P.user1_id = A.user_id AND P.user2_id = B.user_id "
                + "AND P.user1_id = S.user1_id AND P.user2_id = S.user2_id "
                + "AND S.photo_id = PH.photo_id AND PH.album_id = AL.album_id AND rownum < "
                + n );
        /*************************************************************************************/

        Long girlUserId = 0L;
        String girlFirstName = "null";
        String girlLastName = "null";
        int girlYear = 0;
        Long boyUserId = 0L;
        String boyFirstName = "null";
        String boyLastName = "null";
        int boyYear = 0;
        MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName,
                girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
        String sharedPhotoId = "null";
        String sharedPhotoAlbumId = "null";
        String sharedPhotoAlbumName = "null";
        String sharedPhotoCaption = "null";
        String sharedPhotoLink = "null";
        mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));

        try {while (rst.next()) {
            girlUserId = rst.getLong(1);
            girlFirstName = rst.getString(2);
            girlLastName = rst.getString(3);
            girlYear = rst.getInt(4);
            boyUserId = rst.getLong(5);
            boyFirstName = rst.getString(6);
            boyLastName = rst.getString(7);
            boyYear = rst.getInt(8);
            mp = new MatchPair(girlUserId, girlFirstName, girlLastName,
                    girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
            sharedPhotoId = rst.getString(9);
            sharedPhotoAlbumId = rst.getString(10);
            sharedPhotoAlbumName = rst.getString(11);
            sharedPhotoCaption = rst.getString(12);
            sharedPhotoLink = rst.getString(13);
            mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                    sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
            this.bestMatches.add(mp);
        }
            rst = stmt.executeQuery("drop view PhotoCount");
            rst = stmt.executeQuery("drop view SharePhoto");
            rst = stmt.executeQuery("drop view Temp");
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst.close();
        stmt.close();
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
        //Get all the friends of all the users
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

        //Get pairs of people who have the same friends and user1_id is less than user2_id
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

        //Wipe out the pairs who have already friends
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

        //Count the number of friends they share and output the top n
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
                "WHERE ROWNUM <= " + n + "\n" +
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
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        //------------Find the friends of given person-----
        /**************************************************************************************
         create view Friend as
         select A.USER2_id as user_id, B.year_of_birth, B.month_of_birth, B.day_of_birth
         from yjtang.public_FRIENDS A, yjtang.public_USERS B
         WHERE A.USER1_ID = 215 AND A.USER2_id = B.user_id
         UNION
         select A.USER1_id as user_id, B.year_of_birth, B.month_of_birth, B.day_of_birth
         from yjtang.public_FRIENDS A, yjtang.public_USERS B
         WHERE A.USER2_ID = 215 AND A.USER1_id = B.user_id;
        */
        ResultSet rst = stmt.executeQuery("create or replace view Friend as "
                + "select A.USER2_id as user_id, B.year_of_birth, B.month_of_birth, "
                + "B.day_of_birth from " + friendsTableName + " A, " +  userTableName + " B "
                + "WHERE A.USER1_ID = " + user_id + " AND A.USER2_id = B.user_id UNION "
                + "select A.USER1_id as user_id, B.year_of_birth, B.month_of_birth, B.day_of_birth  from "
                + friendsTableName + " A, " + userTableName+ " B WHERE A.USER2_ID = "
                + user_id + " AND A.USER1_id = B.user_id");
        /*************************************************************************************/

        //	---------Find the oldest friend------------------
        //	---------find the oldest year--------------------
        /**************************************************************************************
         create view Friend_year as
         select user_id, month_of_birth, day_of_birth from Friend
         where year_of_birth =
         ( select MIN(year_of_birth) from Friend );
        */
        rst = stmt.executeQuery("create or replace view Friend_year as "
                + "select user_id, month_of_birth, day_of_birth from Friend where "
                + "year_of_birth = ( select MIN(year_of_birth) from Friend )");
        /*************************************************************************************/

        //---------find the oldest month--------------------
        /**************************************************************************************
         create view Friend_month as
         select user_id, day_of_birth from Friend_year
         where month_of_birth =
         ( select MIN(month_of_birth) from Friend_year);
        */
        rst = stmt.executeQuery("create or replace view Friend_month as "
                + "select user_id, day_of_birth from Friend_year where "
                + "month_of_birth = ( select MIN(month_of_birth) from Friend_year)");
        /*************************************************************************************/

        //	---------find the oldest day--------------------
        /**************************************************************************************
         create view Friend_day as
         select user_id from Friend_month
         where day_of_birth =
         ( select MIN(day_of_birth) from Friend_month);
        */
        rst = stmt.executeQuery("create or replace view Friend_day as "
                + "select user_id from Friend_month where "
                + "day_of_birth = ( select MIN(day_of_birth) from Friend_month)");
        /*************************************************************************************/

        //	---------find the larger user_id--------------------
        /**************************************************************************************
         create view Friend_id as
         select user_id from Friend_day
         where user_id =
         ( select MAX(user_id) from Friend_day);
        */
        rst = stmt.executeQuery("create or replace view Friend_id as "
                + "select user_id from Friend_day where user_id = "
                + "( select MAX(user_id) from Friend_day)");
        /*************************************************************************************/

        //	----------print out----------------------------
        /**************************************************************************************
         select A.first_name, A.LAST_NAME, A.user_id from yjtang.public_USERS A, Friend_id B
         WHERE A.user_id = B.USER_ID;
        */
        rst = stmt.executeQuery("select A.first_name, A.LAST_NAME, A.user_id from "
                + userTableName + " A, Friend_id B WHERE A.user_id = B.USER_ID");
        /*************************************************************************************/

        try {
            while (rst.next()) {
                this.oldestFriend = new UserInfo(rst.getLong(3), rst.getString(1), rst.getString(2));
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }


        //	---------Find the youngest friend------------------
        //	---------find the youngest year--------------------
        /**************************************************************************************
         create view Friend_year as
         select user_id, month_of_birth, day_of_birth from Friend
         where year_of_birth =
         ( select MAX(year_of_birth) from Friend );
        */
        rst = stmt.executeQuery("create or replace view Friend_year as "
                + "select user_id, month_of_birth, day_of_birth from Friend where "
                + "year_of_birth = ( select MAX(year_of_birth) from Friend )");
        /*************************************************************************************/

        //---------find the youngest month--------------------
        /**************************************************************************************
         create view Friend_month as
         select user_id, day_of_birth from Friend_year
         where month_of_birth =
         ( select MAX(month_of_birth) from Friend_year);
        */
        rst = stmt.executeQuery("create or replace view Friend_month as "
                + "select user_id, day_of_birth from Friend_year where "
                + "month_of_birth = ( select MAX(month_of_birth) from Friend_year)");
        /*************************************************************************************/

        //	---------find the youngest day--------------------
        /**************************************************************************************
         create view Friend_day as
         select user_id from Friend_month
         where day_of_birth =
         ( select MAX(day_of_birth) from Friend_month);
        */
        rst = stmt.executeQuery("create or replace view Friend_day as "
                + "select user_id from Friend_month where "
                + "day_of_birth = ( select MAX(day_of_birth) from Friend_month)");
        /*************************************************************************************/

        //	---------find the smaller user_id--------------------
        /**************************************************************************************
         create view Friend_id as
         select user_id from Friend_day
         where user_id =
         ( select MIN(user_id) from Friend_day);
        */
        rst = stmt.executeQuery("create or replace view Friend_id as "
                + "select user_id from Friend_day where user_id = "
                + "( select MIN(user_id) from Friend_day)");
        /*************************************************************************************/

        //	----------print out----------------------------
        /**************************************************************************************
         select A.first_name, A.LAST_NAME, A.user_id from yjtang.public_USERS A, Friend_id B
         WHERE A.user_id = B.USER_ID;
        */
        rst = stmt.executeQuery("select A.first_name, A.LAST_NAME, A.user_id from "
                + userTableName + " A, Friend_id B WHERE A.user_id = B.USER_ID");
        /*************************************************************************************/

        try {
            while (rst.next()) {
                this.youngestFriend = new UserInfo(rst.getLong(3), rst.getString(1), rst.getString(2));
            }
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst = stmt.executeQuery("Drop view Friend_id");
        rst = stmt.executeQuery("Drop view Friend_day");
        rst = stmt.executeQuery("Drop view Friend_month");
        rst = stmt.executeQuery("Drop view Friend_year");
        rst = stmt.executeQuery("Drop view Friend");

        rst.close();
        stmt.close();
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
        //Get the city and the number of events it holds
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

        //Find the name of all the cities which hold the maximun number of events
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
        Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        /**************************************************************************************
         select A.user_id AS USER1_id, B.user_id AS USER2_id
         from yjtang.public_USERS A, yjtang.public_USERS B, yjtang.public_USER_HOMETOWN_CITY C, yjtang.public_USER_HOMETOWN_CITY D
         WHERE A.user_id < B.user_id AND A.LAST_NAME = B.LAST_NAME
           AND A.user_id = C.user_id AND B.user_id = D.user_id
           AND C.HOMETOWN_CITY_ID = D.HOMETOWN_CITY_ID
           AND abs(A.year_of_birth - B.year_of_birth) < 10
         Intersect
         select USER1_id, USER2_id from yjtang.public_FRIENDS
         order by USER1_id, USER2_id;
        */
        ResultSet rst = stmt.executeQuery("Select Temp.user1_id, A.first_name, A.last_name, Temp.user2_id, "
                + " B.first_name, B.last_name from " + userTableName + " A, "
                + userTableName + " B, (select A.user_id AS user1_id, B.user_id AS user2_id from "
                + userTableName + " A," + userTableName + " B, " + hometownCityTableName + " C, "
                + hometownCityTableName + " D WHERE A.user_id < B.user_id AND A.LAST_NAME = B.LAST_NAME "
                + "AND A.user_id = C.user_id AND B.user_id = D.user_id "
                + "AND C.HOMETOWN_CITY_ID = D.HOMETOWN_CITY_ID "
                + "AND abs(A.year_of_birth - B.year_of_birth) < 10 Intersect "
                + "select USER1_id, USER2_id from " + friendsTableName + " order by user1_id, "
                + "user2_id ) Temp where Temp.user1_id = A.user_id "
                + "and Temp.user2_id = B.user_id");

        try {
            while (rst.next()) {
                SiblingInfo s = new SiblingInfo(rst.getLong(1), rst.getString(2),
                        rst.getString(3), rst.getLong(4), rst.getString(5), rst.getString(6));
                this.siblings.add(s);}
        } catch (SQLException e) { /* print out an error message.*/
            closeEverything(rst, stmt);
        }

        rst.close();
        stmt.close();
	}
	
}
