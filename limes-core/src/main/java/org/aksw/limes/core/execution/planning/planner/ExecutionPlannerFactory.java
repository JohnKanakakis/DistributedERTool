package org.aksw.limes.core.execution.planning.planner;


import org.aksw.limes.core.io.cache.Cache;
import org.apache.log4j.Logger;

public class ExecutionPlannerFactory {
    public static final String DEFAULT = "Default";//"canonical";
    public static final String HELIOS = "helios";
    public static final String DYNAMIC = "dynamic";
    private static final Logger logger = Logger.getLogger(ExecutionPlannerFactory.class.getName());

    /**
     * @param name,
     *            type of the Execution Planner
     * @return a specific execution engine instance
     * @author kleanthi
     */
    public static IPlanner getPlanner(String name, Cache source, Cache target) {

    //logger.info("planner name = "+name);
	if (name.equalsIgnoreCase(DEFAULT))
	    return new CanonicalPlanner();
	if (name.equalsIgnoreCase(HELIOS))
	    return new HeliosPlanner(target, target);
	//if (name.equalsIgnoreCase(DYNAMIC))
	//   return new DynamicPlanner(source, target);

	logger.error("Sorry, " + name + " is not yet implemented. Exit with error ...");
	System.exit(1);
	return null;
    }
}
