package com.ktb.chatapp.websocket.socketio;

/**
 * Socket User Record
 * 
 * @param id            user id
 * @param name          user name
 * @param email         user email
 * @param profileImage  user profileImage
 * @param authSessionId user auth session id
 * @param socketId      user websocket session id
 */
public record SocketUser(String id, String name, String email, String profileImage, String authSessionId,
                String socketId) {
}
