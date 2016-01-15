/**
 * 
 */
package mem.test;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Factory to generate populated model objects for testing purposes.
 * 
 */
public class TestObjectFactory {
	private static Log log = LogFactory.getLog(TestObjectFactory.class);

	private static final InvocationHandler invoker = new InvocationHandler() {
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			return TestObjectFactory.getObject(method.getReturnType());
		}
	};

	/**
	 * Instantiates and populates a list of size <code>count</code> with
	 * randomly generated objects of type <code>T</code>
	 */
	public static <T> List<T> getObject(Class<T> clazz, int count) {
		return getObject(clazz, count, null);
	}

	public static <T> List<T> getObject(Class<T> clazz, int count, T template) {
		List<T> list = new ArrayList<T>();

		if (count > 0) {
			for (int i = 0; i < count; i++) {
				T object = getObject(clazz, template);
				list.add(object);
			}
		}

		return list;
	}

	/**
	 * Instantiates and populates an object of type T, overriding values with
	 * value supplied by template
	 */
	public static <T> T getObject(Class<T> clazz, T template) {
		// Only applies "top-level" templating right now. Does not recurse.
		T object = getObject(clazz);

		// Woooo! Nesting....
		if (template != null) {
			Method[] methods = clazz.getMethods();
			for (Method getter : methods) {
				if (getter.getName().startsWith("get") && getter.getParameterTypes().length == 0) {
					try {
						Object got = getter.invoke(template);
						if (got != null) {
							String setterName = "set" + StringUtils.substring(getter.getName(), 3);
							try {
								Method setter = clazz.getMethod(setterName, getter.getReturnType());
								if (getter.getReturnType().isPrimitive()) {
									if (shouldUsePrimitive(got, setter.getParameterTypes()[0])) {
										setter.invoke(object, got);
									}
								} else {
									setter.invoke(object, got);
								}
							} catch (NoSuchMethodException e) {
								log.debug("No setter found for " + setterName + ":"
										+ getter.getReturnType());
							}
						}
					} catch (Exception e) {
						throw new IllegalArgumentException("Problem invoking getter: "
								+ getter.getName(), e);
					}
				}
			}
		}

		return object;
	}

	/**
	 * Determines whether to use the value provided in the template. Returns
	 * false if the primitive value is the same as the default value for that
	 * type (0, false, etc)
	 */
	private static boolean shouldUsePrimitive(Object value, Class<?> type) {
		// basically we're going to say DON'T copy the value if it is the
		// primitive default

		boolean retVal = true;
		if (type == Long.TYPE || //
				type == Integer.TYPE || //
				type == Short.TYPE || //
				type == Byte.TYPE || //
				type == Double.TYPE || //
				type == Float.TYPE) {
			if (((Number) value).intValue() == 0) {
				retVal = false;
			}
		} else if (type == Boolean.TYPE && ((Boolean) value).booleanValue() == false) {
			retVal = false;
		} else if (type == Double.TYPE && ((Double) value).intValue() == 0) {
			retVal = false;
		} else if (type == Float.TYPE && ((Float) value).intValue() == 0) {
			retVal = false;
		} else if (type == Character.TYPE && ((Character) value).charValue() == 0) {
			retVal = false;
		}

		return retVal;
	}

	/**
	 * @param <T>
	 *            Generic type to instantiate and return
	 * @param clazz
	 *            Class definition to retrieve setters from
	 * @return A randomly populated instance of class T
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getObject(Class<T> clazz) {
		// TODO looking into java.beans, some of the introspecition stuff may
		// have made this easier
		// TODO Handle circular references
		// TODO Pick random enumeration rather than [0]

		T object = null;
		if (clazz.isInterface()) {
			object = handleInterface(clazz);
		} else if (clazz.isAnnotation()) {
			// Seriously? WTF buddy?
			throw new IllegalStateException("Unable to handle annotation: " + clazz);
		} else if (clazz.isArray()) {
			object = (T) Array.newInstance(clazz.getComponentType(), 0);
		} else if (clazz.isEnum()) {
			object = clazz.getEnumConstants()[0]; // should we use
													// getRandomEnumConstant()
													// instead?
		} else if (clazz.isPrimitive()) {
			object = (T) handlePrimitive(clazz);
		} else {
			object = handleClass(clazz);
		}

		return object;
	}

	@SuppressWarnings("unchecked")
	public static <T> T handleInterface(Class<T> clazz) {
		Object proxy = Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] { clazz },
				invoker);
		return (T) proxy;
	}

	private static <T> T handleClass(Class<T> clazz) {
		T object;
		try {
			object = clazz.newInstance();
		} catch (Exception e) {
			throw new IllegalStateException("Unable to instantiate class: " + clazz, e);
		}

		Method[] methods = clazz.getMethods();
		for (Method method : methods) {
			if (method.getName().startsWith("set") && method.getParameterTypes().length == 1
					&& method.getReturnType() == Void.TYPE) {
				try {
					if (isPrimitive(method.getParameterTypes()[0])) {
						Object o = handlePrimitive(method.getParameterTypes()[0]);
						method.invoke(object, o);
					} else if (method.getParameterTypes()[0] == UUID.class) {
						method.invoke(object, UUID.randomUUID());
					} else if (method.getParameterTypes()[0] == Date.class) {
						method.invoke(object, DateUtils.truncate(new Date(), Calendar.DATE));
					} else if (method.getParameterTypes()[0] == Calendar.class) {
						method.invoke(object,
								DateUtils.truncate(Calendar.getInstance(), Calendar.DATE));
					} else if (method.getParameterTypes()[0] == String.class) {
						method.invoke(object, getRandomString(10));
					} else if (method.getParameterTypes()[0] == List.class) {
						method.invoke(object, new ArrayList<Object>());
					} else if (method.getParameterTypes()[0] == Set.class) {
						method.invoke(object, new HashSet<Object>());
					} else if (method.getParameterTypes()[0] == Map.class) {
						method.invoke(object, new HashMap<Object, Object>());
					} else if (method.getParameterTypes()[0].isEnum()) {
						Class<?> c = method.getParameterTypes()[0];
						method.invoke(object, c.getEnumConstants()[0]);
					} else if (method.getParameterTypes()[0].isArray()) {
						Object array = Array.newInstance(
								method.getParameterTypes()[0].getComponentType(), 0);
						method.invoke(object, array);
					} else if (method.getParameterTypes()[0].getPackage().getName()
							.startsWith("com.jpmc.vpc")) {
						Object o = getObject(method.getParameterTypes()[0]);
						method.invoke(object, o);
					} else {
						if (log.isDebugEnabled()) {
							log.debug("property not set - " + method.getName() + " : "
									+ Arrays.asList(method.getParameterTypes()));
						}
					}
				} catch (Exception e) {
					String message = String.format("Unable to invoke method %s on class %s",
							method.getName(), clazz);
					throw new IllegalStateException(message, e);
				}
			}
		}
		return object;
	}

	/**
	 * isPrimitive-ish. Close enough anyways.
	 */
	private static boolean isPrimitive(Class<?> c) {
		if (c.isPrimitive()) {
			return true;
		} else if (c == Integer.class //
				|| c == Long.class //
				|| c == Boolean.class //
				|| c == Character.class //
				|| c == Byte.class //
				|| c == Short.class //
				|| c == Float.class //
				|| c == Double.class) {
			return true;
		} else {
			return false;
		}
	}

	// private static boolean handlePrimitiveAndWrapper(Method method, Object
	// object) throws IllegalArgumentException, IllegalAccessException,
	// InvocationTargetException {
	// Class<?> c = method.getParameterTypes()[0];
	// if (c == Integer.TYPE || c == Integer.class) {
	// method.invoke(object, getRandomInt(3));
	// } else if (c == Long.TYPE || c == Long.class) {
	// method.invoke(object, getRandomLong(8));
	// } else if (c == Boolean.TYPE || c == Boolean.class) {
	// method.invoke(object, RandomUtils.nextBoolean());
	// } else if (c == Character.TYPE || c == Character.class) {
	// method.invoke(object, (char) getRandomInt(2));
	// } else if (c == Byte.TYPE || c == Byte.class) {
	// method.invoke(object, (byte) getRandomInt(2));
	// } else if (c == Short.TYPE || c == Short.class) {
	// method.invoke(object, (short) getRandomInt(3));
	// } else if (c == Float.TYPE || c == Float.class) {
	// method.invoke(object, RandomUtils.nextFloat() * 100);
	// } else if (c == Double.TYPE || c == Double.class) {
	// method.invoke(object, RandomUtils.nextDouble() * 1000);
	// } else {
	// return false;
	// }
	//
	// return true;
	// }

	private static Object handlePrimitive(Class<?> c) {
		if (c == Integer.TYPE || c == Integer.class) {
			return Integer.valueOf(getRandomInt(3));
		} else if (c == Long.TYPE || c == Long.class) {
			return Long.valueOf(getRandomLong(8));
		} else if (c == Boolean.TYPE || c == Boolean.class) {
			return Boolean.valueOf(RandomUtils.nextBoolean());
		} else if (c == Character.TYPE || c == Character.class) {
			return Character.valueOf((char) getRandomInt(2));
		} else if (c == Byte.TYPE || c == Byte.class) {
			return Byte.valueOf((byte) getRandomInt(2));
		} else if (c == Short.TYPE || c == Short.class) {
			return Short.valueOf((short) getRandomInt(3));
		} else if (c == Float.TYPE || c == Float.class) {
			return Float.valueOf(RandomUtils.nextFloat() * 100);
		} else if (c == Double.TYPE || c == Double.class) {
			return Double.valueOf(RandomUtils.nextDouble() * 1000);
		} else {
			throw new IllegalStateException("Unable to handle primitive for " + c);
		}
	}

	public static String getRandomString(int size) {
		return RandomStringUtils.randomAlphabetic(size);
	}

	public static long getRandomLong(int size) {
		long length = (long) Math.pow(10, size);
		return (long) (Math.random() * length);
	}

	public static int getRandomInt(int size) {
		return (int) getRandomLong(size);
	}

	/**
	 * Returns a random enum constant.
	 * 
	 * @param enumType
	 *            the enum type
	 * @param includeNulls
	 *            if true, nulls with be returned with frequency equal to that
	 *            of any constant
	 * @return a random enum constant value, or null if includeNulls is true
	 */
	public static <T extends Enum<T>> T getRandomEnumConstant(Class<T> enumType,
			boolean includeNulls) {
		T[] ta = enumType.getEnumConstants();
		if (ta == null) {
			throw new IllegalArgumentException(enumType + " not an enum type");
		} else if (ta.length == 0) {
			if (includeNulls) {
				return null;
			}
			throw new IllegalArgumentException("no type constants for " + enumType);
		} else {
			if (includeNulls) {
				int i = RandomUtils.nextInt(ta.length + 1);
				return i == ta.length ? null : ta[i];
			}
			return ta[RandomUtils.nextInt(ta.length)];
		}
	}
}
