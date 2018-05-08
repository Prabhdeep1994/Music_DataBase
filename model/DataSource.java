package com.prabhdeep_singh.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    public static final String DB_Name = "musicdatabase.db";

    public static final String DB_Connection = "jdbc:sqlite:C:\\Users\\Prabhdeep\\Downloads\\MusicDatabase\\" + DB_Name;

    public static final String Table_Album = "albums";
    public static final String Column_Album_Id = "_id";
    public static final String Column_Album_Name = "name";
    public static final String Column_Album_Artist = "artist";
    public static final int Index_Album_id = 1;
    public static final int Index_Album_Name = 2;
    public static final int Index_Album_Artist = 3;

    public static final String Table_Artists = "artists";
    public static final String Column_Artist_Id = "_id";
    public static final String Column_Artist_Name = "name";
    public static final int Index_Artist_Id = 1;
    public static final int Index_Artist_Name = 2;

    public static final String Table_Songs = "songs";
    public static final String Column_Songs_Id = "_id";
    public static final String Column_Songs_Track = "track";
    public static final String Column_Songs_Title = "title";
    public static final String Column_Songs_Album = "album";
    public static final int Index_Songs_Id = 1;
    public static final int Index_Songs_Track = 2;
    public static final int Index_Songs_Title = 3;
    public static final int Index_Songs_Album = 4;

    public static final int Order_by_None = 1;
    public static final int Order_by_Asc = 2;
    public static final int Order_by_Desc = 3;

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + Table_Album + '.' + Column_Album_Name + " FROM " +
                    Table_Album + " INNER JOIN " + Table_Artists +
                    " ON " + Table_Album + "." + Column_Album_Artist +
                    " = " + Table_Artists + "." + Column_Artist_Id +
                    " WHERE " + Table_Artists + "." + Column_Artist_Name + " = \"";

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + Table_Album + "." + Column_Album_Name + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_FOR_SONGS_TITLE=
            "SELECT " +Table_Artists + '.' + Column_Artist_Name + ", " +
                    Table_Album + '.' + Column_Album_Name + ", " + Table_Songs + '.' +
                    Column_Songs_Track + " FROM " + Table_Songs
                    + " INNER JOIN " + Table_Album + " ON " + Table_Songs + "."
                    + Column_Songs_Album + " = " + Table_Album + "." + Column_Album_Id
                    +" INNER JOIN "+ Table_Artists + " ON " + Table_Album
                    +'.' + Column_Album_Artist + " = " + Table_Artists + '.'
                    + Column_Artist_Id + " WHERE " + Table_Songs + "."
                    + Column_Songs_Title + " = \"";

    public static final String QUERY_ARTIST_FOR_SONGS_SORT =
            " ORDER BY " + Table_Album + "." + Column_Album_Name + ", "
                    + Table_Artists + "." + Column_Artist_Name
                    + " COLLATE NOCASE ";

    private static final String TABLE_ARTIST_SONG_VIEW ="artist_list" ;

    public static final String CREATE_ARTIST_FOR_SONG_VIEW =
            "CREATE VIEW IF NOT EXISTS " +
                    TABLE_ARTIST_SONG_VIEW + " AS SELECT " + Table_Artists + "." + Column_Artist_Name + ", " +
                    Table_Album + "." + Column_Album_Name + " AS " + Column_Songs_Album + ", " +
                    Table_Songs + "." + Column_Songs_Track + ", " + Table_Songs + "." + Column_Songs_Title +
                    " FROM " + Table_Songs + " INNER JOIN " + Table_Album + " ON " + Table_Songs +
                    "." + Column_Songs_Album + " = " + Table_Album + "." + Column_Album_Id +
                    " INNER JOIN " + Table_Artists + " ON " + Table_Album + "." + Column_Album_Artist +
                    " = " + Table_Artists + "." + Column_Artist_Id +
                    " ORDER BY " + Table_Artists + "." + Column_Artist_Name + ", " +
                    Table_Album + "." + Column_Album_Name + ", " +
                    Table_Songs + "." + Column_Songs_Track;

    public static final String QUERY_VIEW_SONGS_TITLE =
            "Select " + Column_Artist_Name + ", " + Column_Album_Name + ", "
                     + Column_Songs_Track  + " FROM " + CREATE_ARTIST_FOR_SONG_VIEW
                        + " Where " + Column_Songs_Title + " =\" ";

    public static final String QUERY_VIEW_SONGS_TITLE_PREPARED_STATEMENT =
            "Select " + Column_Artist_Name + ", " + Column_Album_Name + ", "
                    + Column_Songs_Track  + " FROM " + CREATE_ARTIST_FOR_SONG_VIEW
                    + " Where " + Column_Songs_Title + " = ? ";

    public static final String Insert_Into_Artist = "Insert Into " + Table_Artists +
                     '(' + Column_Artist_Name + " ) Values(?)";

    public static final String Insert_Into_Album = "Insert Into " + Table_Album +
                       '(' + Column_Album_Name + ", " +Column_Album_Artist
                            + " ) Values(?, ?)";

    public static final String Insert_Into_Songs = "Insert Into " + Table_Songs +
                       '(' + Column_Songs_Track +", " + Column_Songs_Title + ", "+
                        Column_Songs_Album + " ) Values(?, ?, ?)";

    public static final String Query_Artist = "Select " + Column_Artist_Id +
                        " From " + Table_Artists + " Where " + Column_Artist_Name
                            + " = ?";

    public static final String Query_Album = "Select " + Column_Album_Id + " From "
                        + Table_Album + " Where " + Column_Album_Name + " = ?";

    private Connection connection;

    private PreparedStatement querySongFromArtistList;

    private PreparedStatement insertIntoArtist;
    private PreparedStatement insertIntoAlbum;
    private PreparedStatement insertIntoSongs;

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    public boolean open() {
        try {
            connection = DriverManager.getConnection(DB_Connection);
            querySongFromArtistList = connection.prepareStatement(QUERY_VIEW_SONGS_TITLE_PREPARED_STATEMENT);
            insertIntoArtist = connection.prepareStatement(Insert_Into_Artist, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbum = connection.prepareStatement(Insert_Into_Album, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = connection.prepareStatement(Insert_Into_Songs);
            queryArtist = connection.prepareStatement(Query_Artist);
            queryAlbum = connection.prepareStatement(Query_Album);
            return true;
        } catch (SQLException e) {
            System.out.println("Connection failure: " + e.getMessage());
            return false;
        }
    }

    public boolean close() {
        try {
            if(querySongFromArtistList != null){
                querySongFromArtistList.close();
            }
            if(insertIntoArtist != null){
                insertIntoArtist.close();
            }
            if(insertIntoAlbum != null){
                insertIntoAlbum.close();
            }
            if(insertIntoSongs != null){
                insertIntoSongs.close();
            }
            if(queryArtist != null){
                queryArtist.close();
            }
            if(queryAlbum != null){
                queryAlbum.close();
            }
            if (connection != null)
                connection.close();
            return true;
        } catch (SQLException e) {
            System.out.println("Unable to close the connection: " + e.getMessage());
            return false;
        }
    }

    public List<Artist> queryArtists(int sortOrder) {

        StringBuilder sb = new StringBuilder("Select * FROM ");
        sb.append(Table_Artists);
        if (sortOrder != Order_by_None) {
            sb.append(" Order By ");
            sb.append(Column_Artist_Name);
            sb.append(" Collate NoCase ");
            if (sortOrder == Order_by_Desc) {
                sb.append(" DESC ");
            } else {
                sb.append(" ASC ");
            }
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sb.toString())) {

            List<Artist> artists = new ArrayList<>();
            while (resultSet.next()) {
                Artist artist = new Artist();
                artist.setId(resultSet.getInt(Index_Artist_Id));
                artist.setName(resultSet.getString(Index_Artist_Name));
                artists.add(artist);
            }
            return artists;
        } catch (SQLException e) {
            System.out.println("Artist Query Failed: " + e.getMessage());
            return null;
        }

    }

/*
    public List<String> queryAlbumsForArtist(String artistName, int sortOrder) {

        StringBuilder sb = new StringBuilder(" Select ");
        sb.append(Table_Album);
        sb.append('.');
        sb.append(Column_Album_Name);
        sb.append(" From ");
        sb.append(Table_Album);
        sb.append(" Inner Join ");
        sb.append(Table_Artists);
        sb.append(" On ");
        sb.append(Table_Album);
        sb.append('.');
        sb.append(Column_Album_Artist);
        sb.append(" = ");
        sb.append(Table_Artists);
        sb.append('.');
        sb.append(Column_Artist_Id);
        sb.append(" Where ");
        sb.append(Table_Artists);
        sb.append('.');
        sb.append(Column_Artist_Name);
        sb.append(" =\" ");
        sb.append(artistName);
        sb.append(" \"");

        if (sortOrder != Order_by_None) {
            sb.append(" Order By ");
            sb.append(Table_Album);
            sb.append('.');
            sb.append(Column_Album_Name);
            sb.append(" Collate NoCase ");
            if (sortOrder == Order_by_Desc) {
                sb.append(" DESC ");
            } else {
                sb.append(" ASC ");
            }
        }

        System.out.println("SQL Statement: " + sb.toString());

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sb.toString())) {

            List<String> album = new ArrayList<>();
            while (resultSet.next()) {
                album.add(resultSet.getString(1));
            }
            return album;
        } catch (SQLException e) {
            System.out.println("Albums query Failed: " + e.getMessage());
            return null;
        }


    }
*/
    public List<String> queryAlbumsForArtist(String artistName, int sortOrder) {

        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artistName);
        sb.append("\"");

        if(sortOrder != Order_by_None) {
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if(sortOrder == Order_by_Desc) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL statement = " + sb.toString());

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while(results.next()) {
                albums.add(results.getString(1));
            }

            return albums;

        } catch(SQLException e) {
            System.out.println("Query failed: "+ e.getMessage());
            return null;
        }


    }
    public List<SongsArtist> queryArtistForSongs(String songName, int sortOrder) {

        StringBuilder sb = new StringBuilder(QUERY_ARTIST_FOR_SONGS_TITLE);
        sb.append(songName);
        sb.append("\"");

        if (sortOrder != Order_by_None) {
            sb.append(QUERY_ARTIST_FOR_SONGS_SORT);
            if (sortOrder == Order_by_Desc) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL statement = " + sb.toString());

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<SongsArtist> songsArtists = new ArrayList<>();

            while (results.next()) {

                SongsArtist songsArtist = new SongsArtist();
                songsArtist.setArtistName(results.getString(1));
                songsArtist.setAlbumName(results.getString(2));
                songsArtist.setTrack(results.getInt(3));
                songsArtists.add(songsArtist);
            }
            return songsArtists;

        } catch (SQLException e) {
            System.out.println("Songs Title Query Failed: " + e.getMessage());
            return null;
        }
    }
    public void querySongsMetaData(){
        String sqlSongsMetaData = "Select * From " + Table_Songs;

        try(Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlSongsMetaData)){

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int numColumns = resultSetMetaData.getColumnCount();
            for(int i=1; i<=numColumns; i++){
                System.out.println(" Columns in the songs table " + i + ". "+ resultSetMetaData.getColumnName(i));
            }
        } catch (SQLException e) {
            System.out.println("MetaData Query Failed: " + e.getMessage());
        }

    }
    public int getCount(String tableName){
        String sqlCount = "Select COUNT(*) From " + tableName;
        try(Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlCount)){

            int count = resultSet.getInt(1);
            return count;
        } catch (SQLException e) {
            System.out.println("Count Query Failed: " + e.getMessage());
            return -1;
        }
    }
    public boolean createViewForSongArtists() {
        try(Statement statement = connection.createStatement()) {

            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        } catch(SQLException e) {
            System.out.println("Create View Query failed: " + e.getMessage());
            return false;
        }
    }
/*
    public List<SongsArtist> querySongViewTitle (String title){

        StringBuilder sb = new StringBuilder(QUERY_ARTIST_FOR_SONGS_TITLE);
        sb.append(title);
        sb.append("\"");

        System.out.println("SQL statement = " + sb.toString());

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<SongsArtist> songsArtists = new ArrayList<>();

            while (results.next()) {
                SongsArtist songsArtist = new SongsArtist();
                songsArtist.setArtistName(results.getString(1));
                songsArtist.setAlbumName(results.getString(2));
                songsArtist.setTrack(results.getInt(3));
                songsArtists.add(songsArtist);
            }
            return songsArtists;

        } catch (SQLException e) {
            System.out.println("Artist_list table Title Query Failed: " + e.getMessage());
            return null;
        }
    }
*/

    public List<SongsArtist> querySongFromArtistList (String title){

        try {
            querySongFromArtistList.setString(1, title);
             ResultSet results = querySongFromArtistList.executeQuery();

            List<SongsArtist> songsArtists = new ArrayList<>();
            while (results.next()) {
                SongsArtist songsArtist = new SongsArtist();
                songsArtist.setArtistName(results.getString(1));
                songsArtist.setAlbumName(results.getString(2));
                songsArtist.setTrack(results.getInt(3));
                songsArtists.add(songsArtist);
            }
            return songsArtists;

        } catch (SQLException e) {
            System.out.println("Artist_list table Title Query Failed: " + e.getMessage());
            return null;
        }
    }
    private int insertArtist(String name) throws SQLException {

        queryArtist.setString(1, name);
        ResultSet results = queryArtist.executeQuery();
        if(results.next()) {
            return results.getInt(1);
        } else {
            // Inserting artist
            insertIntoArtist.setString(1, name);
            int affectedRows = insertIntoArtist.executeUpdate();

            if(affectedRows != 1) {
                throw new SQLException("Unable to add artist!");
            }

            ResultSet generatedKeys = insertIntoArtist.getGeneratedKeys();
            if(generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for artist");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException {

        queryAlbum.setString(1, name);
        ResultSet results = queryAlbum.executeQuery();
        if(results.next()) {
            return results.getInt(1);
        } else {
            // Inserting album
            insertIntoAlbum.setString(1, name);
            insertIntoAlbum.setInt(2, artistId);
            int affectedRows = insertIntoAlbum.executeUpdate();

            if(affectedRows != 1) {
                throw new SQLException("Unable to add album!");
            }

            ResultSet generatedKeys = insertIntoAlbum.getGeneratedKeys();
            if(generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for album");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {

        try {
            connection.setAutoCommit(false);

            //Inserting song
            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);
            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, albumId);
            int affectedRows = insertIntoSongs.executeUpdate();
            if(affectedRows == 1) {
                connection.commit();
            } else {
                throw new SQLException("Inserting song query failed");
            }

        } catch(Exception e) {
            System.out.println("Insert song exception: " + e.getMessage());
            try {
                System.out.println("Performing rollback");
                connection.rollback();
            } catch(SQLException e2) {
                System.out.println("Rollback issue occur " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting commit.......again to true");
                connection.setAutoCommit(true);
            } catch(SQLException e) {
                System.out.println("Problem occur in  auto-commit! " + e.getMessage());
            }

        }
    }
}




