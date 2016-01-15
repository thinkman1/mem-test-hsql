package mem.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

import org.springframework.context.ApplicationContext;

/**
 * Class to help proxying around the app context
 *
 */
public final class AppContextProxy {

	/**
	 * Constructor
	 */
	private AppContextProxy() {
	}

	/**
	 * Method to proxy getBean for testing. <br/><br/> Example:<br/>
	 * <code>ApplicationContext originalContext = SqlContextFactory.getContext();<br/>
	 * Map<String, Object> beanNameToNewObject = new HashMap<String, Object>();<br/>
	 * beanNameToNewObject.put("getIcreCreditRefId", "SELECT NEXT VALUE FOR CREDIT_REFERENCE_ID_SEQ FROM DUAL");<br/>
	 * ApplicationContext newContext = AppContextProxy.proxyGetBean(originalContext, beanNameToNewObject);<br/>
	 * SqlContextFactory.setContext(newContext);<br/>
	 </code>
	 *
	 * @param myOriginalAppContext
	 *            the original application context
	 * @param beanNameToNewObjectMap
	 *            the map of bean name to object to proxy
	 * @return the new app context to set on your current app context factory
	 */
	public static ApplicationContext proxyGetBean(final ApplicationContext myOriginalAppContext,
			final Map<String, Object> beanNameToNewObjectMap) {

		// Build a Proxy
		InvocationHandler handler = new InvocationHandler() {
			public Object invoke(final Object proxy, final Method method, final Object[] args)
					throws IllegalAccessException, IllegalArgumentException,
					InvocationTargetException {
				if ("getBean".equals(method.getName()) && args.length == 1
						&& beanNameToNewObjectMap.containsKey(args[0])) {
					return beanNameToNewObjectMap.get(args[0]);
				}
				return method.invoke(myOriginalAppContext, args);
			}
		};

		ApplicationContext f = (ApplicationContext) Proxy.newProxyInstance(ApplicationContext.class
				.getClassLoader(), new Class[] {ApplicationContext.class}, handler);

		return f;
	}
}
