package de.fzi.cep.sepa.rest.v2;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import de.fzi.cep.sepa.commons.config.ConfigurationManager;
import de.fzi.cep.sepa.commons.config.WebappConfigurationSettings;
import de.fzi.cep.sepa.manager.setup.Installer;
import de.fzi.cep.sepa.messages.ErrorMessage;
import de.fzi.cep.sepa.messages.Notification;
import de.fzi.cep.sepa.messages.NotificationType;
import de.fzi.cep.sepa.messages.Notifications;
import de.fzi.cep.sepa.messages.SuccessMessage;
import de.fzi.cep.sepa.model.client.user.RegistrationData;
import de.fzi.cep.sepa.model.client.user.Role;
import de.fzi.cep.sepa.model.client.user.ShiroAuthenticationRequest;
import de.fzi.cep.sepa.model.client.user.ShiroAuthenticationResponse;
import de.fzi.cep.sepa.model.client.user.ShiroAuthenticationResponseFactory;
import de.fzi.cep.sepa.model.client.user.User;
import de.fzi.cep.sepa.rest.api.AbstractRestInterface;
import de.fzi.cep.sepa.rest.api.v2.Authentication;
import de.fzi.cep.sepa.storage.controller.StorageManager;

@Path("/v2/admin")
public class AuthenticationImpl extends AbstractRestInterface implements Authentication {

    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
	@Override
	@Path("/login")
	public String doLogin(String token) {
    	Subject subject = SecurityUtils.getSubject();
        if (SecurityUtils.getSubject().isAuthenticated()) return "Already logged in. Please log out to change user";

        ShiroAuthenticationRequest req = new Gson().fromJson(token, ShiroAuthenticationRequest.class);
        UsernamePasswordToken shiroToken = new UsernamePasswordToken(req.getUsername(), req.getPassword());
        shiroToken.setRememberMe(true);
        try {
            subject.login(shiroToken);
            return toJson(ShiroAuthenticationResponseFactory.create((User) userStorage.getUser((String) subject.getPrincipal())));
        } catch (AuthenticationException e) {
            e.printStackTrace();
            return toJson(new ErrorMessage(NotificationType.LOGIN_FAILED.uiNotification()));
        } 
	}

    @Path("/logout")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
	@Override
	public String doLogout(String token) {
		Subject subject = SecurityUtils.getSubject();
        subject.logout();
        return toJson(new SuccessMessage(NotificationType.LOGOUT_SUCCESS.uiNotification()));
	}

    @Path("/register")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
	@Override
	public String doRegister(String registrationData) {
	
    	RegistrationData data = fromJson(registrationData, RegistrationData.class);
		if (StorageManager.INSTANCE.getUserStorageAPI().checkUser(data.getEmail())) {
            return toJson(new ErrorMessage(NotificationType.REGISTRATION_FAILED.uiNotification()));
        }

        Set<Role> roles = new HashSet<Role>();
        roles.add(Role.ADMINISTRATOR);
        roles.add(Role.USER_DEMO);
  
        de.fzi.cep.sepa.model.client.user.User user = new de.fzi.cep.sepa.model.client.user.User(data.getUsername(), data.getEmail(), data.getPassword(), roles);
        userStorage.storeUser(user);
        return toJson(new SuccessMessage(NotificationType.REGISTRATION_SUCCESS.uiNotification()));
	}
    
    @GET
    @Path("/authc")
    @Produces(MediaType.APPLICATION_JSON)
    @Override
    public String userAuthenticated() {
        if (SecurityUtils.getSubject().isAuthenticated()) {
        	 return toJson(ShiroAuthenticationResponseFactory.create((User) userStorage.getUser((String) SecurityUtils.getSubject().getPrincipal())));
             
        }
        return toJson(new ErrorMessage(NotificationType.NOT_LOGGED_IN.uiNotification()));
    }
    
}
