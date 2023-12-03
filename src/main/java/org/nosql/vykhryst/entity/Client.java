package org.nosql.vykhryst.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Client {
    private long id;
    private String username;
    private String firstname;
    private String lastname;
    private String phoneNumber;
    private String email;
    private String password;

    public Client(Builder builder) {
        this.id = builder.id;
        this.username = builder.username;
        this.firstname = builder.firstname;
        this.lastname = builder.lastname;
        this.phoneNumber = builder.phoneNumber;
        this.email = builder.email;
        this.password = builder.password;
    }

    public static class Builder{
        private long id;
        private String username;
        private String firstname;
        private String lastname;
        private String phoneNumber;
        private String email;
        private String password;


        public Builder id(long id){
            this.id = id;
            return this;
        }

        public Builder username(String username){
            this.username = username;
            return this;
        }

        public Builder firstname(String firstname){
            this.firstname = firstname;
            return this;
        }

        public Builder lastname(String lastname){
            this.lastname = lastname;
            return this;
        }

        public Builder phoneNumber(String phoneNumber){
            this.phoneNumber = phoneNumber;
            return this;
        }

        public Builder email(String email){
            this.email = email;
            return this;
        }

        public Builder password(String password){
            this.password = password;
            return this;
        }

        public Client build(){
            return new Client(this);
        }
    }
}
