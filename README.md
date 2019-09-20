# mongodb-sampler

Compile the project

mvn clean package

After compile is finished the jar created must be copied to the lib/ext directory of the JMeter installation home.
Also, in case there are more dependencies that have to be imported they should also be copied to the lib path of the JMeter installation home.

Once the process is complete by adding Java Sampler to a JMeter Thread Group you can choose mongodb sampler.
