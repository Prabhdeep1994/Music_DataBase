package com.prabhdeep_singh;

import com.prabhdeep_singh.model.Artist;
import com.prabhdeep_singh.model.DataSource;
import com.prabhdeep_singh.model.SongsArtist;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        DataSource dataSource = new DataSource();
        if(!dataSource.open()) {
            System.out.println("Cannot open Connection");
            return;
        }

        System.out.println("--------------------------------------------------------");
        List<Artist> artists = dataSource.queryArtists(DataSource.Order_by_Asc);
        if(artists == null){
            System.out.println("No artists");
            return;
        }
        for(Artist artist: artists){
            System.out.println("ID = " + artist.getId() + ", Name = " + artist.getName());
        }

        System.out.println("--------------------------------------------------------");
        List<String> albums = dataSource.queryAlbumsForArtist("Iron Maiden", DataSource.Order_by_Asc);

        for(String album : albums){
            System.out.println(album);
        }

        System.out.println("--------------------------------------------------------");
        List<SongsArtist> songsArtistList = dataSource.queryArtistForSongs("Go Your Own Way" , DataSource.Order_by_Asc);
        if(songsArtistList == null){
            System.out.println("No artists");
            return;
        }
        for(SongsArtist artist : songsArtistList){
            System.out.println("Artist Name = " + artist.getArtistName()
            + "Albums Name = " + artist.getAlbumName()
            + "Songs Track = " + artist.getTrack());
        }

        System.out.println("--------------------------------------------------------");
        dataSource.querySongsMetaData();

        System.out.println("--------------------------------------------------------");
        int count = dataSource.getCount(DataSource.Table_Songs);
        System.out.println("No. of songs in the table " + count);

        System.out.println("--------------------------------------------------------");

        dataSource.createViewForSongArtists();

        System.out.println("--------------------------------------------------------");

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a song title: ");
        String title = scanner.nextLine();

        songsArtistList = dataSource.querySongFromArtistList(title);
        if(songsArtistList == null){
            System.out.println("Unable to find the title");
            return;
        }
        for(SongsArtist artist : songsArtistList){
            System.out.println("Artist Name = " + artist.getArtistName()
                    + "Albums Name = " + artist.getAlbumName()
                    + "Songs Track = " + artist.getTrack());
        }

        dataSource.insertSong("Do You Know", "D Diljit", "High End", 1 );

        dataSource.close();

}
}
