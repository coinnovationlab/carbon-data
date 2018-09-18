package org.wso2.carbon.dataservices.core.odata;

import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;

/**
 * Class to have access to a simple constructor for UriParameter Objects. Used by ODataAdapter for navigation in compound resource paths.
 */
public class NavigationUriParameter implements UriParameter {

	private String name;
	private String text;
	
	public NavigationUriParameter(String name, String text) {
		this.name = name;
		this.text = text;
	}
	
	@Override
	public String getAlias() {
		return null;
	}

	@Override
	public Expression getExpression() {
		return null;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public String getReferencedProperty() {
		return null;
	}

	@Override
	public String getText() {
		return this.text;
	}

	@Override
	public String toString() {
		return name + "=" + text;
	}
}
