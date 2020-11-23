## A (hopefully) Fast, (hopefully) Thread safe map inspired by evmap 

### Maven 
```xml
	<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>
```
```xml
	<dependency>
	    <groupId>com.github.bowbahdoe</groupId>
	    <artifactId>leftright-map-java</artifactId>
	    <version>0.1.1</version>
	</dependency>
```
### Gradle
```groovy
	allprojects {
		repositories {
			...
			maven { url 'https://jitpack.io' }
		}
	}
```

```groovy
	dependencies {
	        implementation 'com.github.bowbahdoe:leftright-map-java:0.1.1'
	}
```

### Leiningen
```clojure
:repositories [["jitpack" "https://jitpack.io"]]
:dependencies [[com.github.bowbahdoe/leftright-map-java "0.1.1"]]	
```