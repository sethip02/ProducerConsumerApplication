package com.application.exception;

/***
 * Runtime exception thrown when there is some issue with the uploading of the chunks
 *
 */
public class UploadException extends RuntimeException{
    public UploadException(String message){
        super(message);
    }
}
