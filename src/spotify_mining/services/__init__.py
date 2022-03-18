import spotipy


class Generic:
    def __init__(
        self, spotify_client_id, spotify_client_secret, user_id, playlist_id, genius_key
    ):
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        self.user_id = user_id
        self.playlist_id = playlist_id
        self.genius_key = genius_key

    def get_playlist_info(self):
        token = spotipy.SpotifyClientCredentials(
            client_id=self.spotify_client_id, client_secret=self.spotify_client_secret
        ).get_access_token()
        sp = spotipy.Spotify(token)
        playlist = sp.user_playlist_tracks(self.user_id, self.playlist_id)
        self.playlist = playlist
        return self.playlist
