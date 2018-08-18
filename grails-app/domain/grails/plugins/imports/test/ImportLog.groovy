package org.oplontis

import java.util.Date;

class ImportLog {

 	String id
	String document
	Date dateCreated
	
	static mapping = {
		version false
		id name: "id", generator: "assigned"
		document sqlType:"longtext"
    }
}
