all:wc1 wlc ngc ngrf

wc1: wcclass
        jar cf wc.jar WordCount*.class

wcclass:
        hadoop com.sun.tools.javac.Main WordCount.java

wlc: wlcclass
        jar cf wlc.jar WordLengthCount*.class

wlcclass:
        hadoop com.sun.tools.javac.Main WordLengthCount.java

ngc: ngcclass
        jar cf ngc.jar NgramCount*.class

ngcclass:
        hadoop com.sun.tools.javac.Main NgramCount.java

ngrf: ngrfclass
        jar cf ngrf.jar NgramRF*.class

ngrfclass:
        hadoop com.sun.tools.javac.Main NgramRF.java

clear:
        rm -rf *.class *.jar
