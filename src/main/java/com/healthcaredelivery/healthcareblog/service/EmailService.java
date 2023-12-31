package com.healthcaredelivery.healthcareblog.service;

import com.healthcaredelivery.healthcareblog.entity.Email;
import org.springframework.stereotype.Service;


public interface EmailService {
    String sendSimpleMail(String toEmail, String subject, String body);
    String sendEmailWithAttachment(Email email);
}
