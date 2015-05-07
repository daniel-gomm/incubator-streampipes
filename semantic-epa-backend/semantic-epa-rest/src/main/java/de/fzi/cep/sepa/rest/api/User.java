package de.fzi.cep.sepa.rest.api;

public interface User {

	public String doRegisterUser();
	
	public String doLoginUser(String username, String password);
	
	public String doLogoutUser();
	
	public String getAllSources();
	
	public String getAllStreams();
	
	public String getAllActions();
	
	public String getSelectedSources();
	
	public String getSelectedStreams();
	
	public String getSelectedActions();
	
}
