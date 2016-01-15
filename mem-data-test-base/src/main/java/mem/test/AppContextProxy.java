package mem.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

import org.springframework.context.ApplicationContext;

/**
 * Class to help proxying around the app context
 * 
 * @author thinkman
 * 
 */
public class AppContextProxy {

	private AppContextProxy() {
	}

	/**
	 * Method to proxy getBean for testing. <br/><br/> Example: <br/>
	 * <code>ApplicationContext originalContext = SqlContextFactory.getContext();<br/>
	 * Map<String, Object> beanNameToNewObject = new HashMap<String, Object>(); <br/>
	 * beanNameToNewObject.put("getDummyAccount", "select acount_no from Account"); <br/>
	 * ApplicationContext newContext = AppContextProxy.proxyGetBean(originakContext, beanNameToNewObject); <br/>
	 * SqlContextFactory.setContext(newContext); <br/>
	 * 
	 * @param myOriginalAppContext
	 * 			the original application context
	 * @param beanNameToNewObjectMap
	 * 			the map of the bean name to object to proxy
	 * @return the new app context to set on your current app context factory
	 */
	public static ApplicationContext proxyGetBean(final ApplicationContext myOriginalAppContext,
			final Map<String, Object> beanNameToNewObjectMap) {

		InvocationHandler handler = new InvocationHandler() {

			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

				if ("getBean".equalsIgnoreCase(method.getName()) && args.length == 1
						&& beanNameToNewObjectMap.containsKey(args[0])) {
					return beanNameToNewObjectMap.get(args[0]);
				}

				return method.invoke(myOriginalAppContext, args);
			}
		};
		ApplicationContext f = (ApplicationContext) Proxy.newProxyInstance(
				AppContextProxy.class.getClassLoader(), new Class[] { ApplicationContext.class },
				handler);

		return f;
	}

}
