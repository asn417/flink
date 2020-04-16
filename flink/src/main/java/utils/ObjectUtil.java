package utils;

/**
 * @Author: wangsen
 * @Date: 2020/4/14 23:01
 * @Description:
 **/
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: LiaoPeng
 * @Date: 2019/12/13
 */
public class ObjectUtil {

    private final static String GET_PREFIX = "get";
    private final static String SET_PREFIX = "set";
    private final static String IS_PREFIX = "is";
    private final static String BOOLEAN = "boolean";

    /**
     * 将对象转换成Map
     * @param obj 需要转换的对象
     * @param <T>
     * @return
     */
    public static <T> Map<String,Object> toMap(final T obj){
        Map<String,Object> map = new HashMap<>();
        if(null == obj) return map;
        Class<?> clazz = obj.getClass();
        //循环父类属性
        while(!clazz.equals(Object.class)){
            Field[] fields = clazz.getDeclaredFields();
            //循环类型属性
            for (Field f : fields) {
                int modifer = f.getModifiers();
                //排除final和static修饰的属性
                if(Modifier.isFinal(modifer)||Modifier.isStatic(modifer)) continue;
                String fieldName = f.getName();
                Method method = null;
                try {
                    String typeName = f.getGenericType().getTypeName();
                    String prefix = BOOLEAN.equals(typeName)?IS_PREFIX:GET_PREFIX;
                    String fName = fieldName.substring(0,1).toUpperCase()+fieldName.substring(1);
                    method = clazz.getMethod(prefix + fName);
                    if(null == method) continue;
                    map.put(fieldName,method.invoke(obj));
                }catch (Exception e){

                }
            }
            clazz = clazz.getSuperclass();
        }
        return map;
    }

    /**
     * 将Map转换为传入的目标类型
     * @param map   传入Map类型数据
     * @param clazz 传入要转换的目标类型
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> T mapToObj(Map<String,Object> map, Class<T> clazz) throws IllegalAccessException, InstantiationException {
        T t = clazz.newInstance();
        if(null == map) return t;
        Class<?> tClass = clazz;
        //循环父类属性
        while(!tClass.equals(Object.class)){
            Field[] fields = tClass.getDeclaredFields();
            //循环类型属性
            for (Field f : fields) {
                int modifer = f.getModifiers();
                //排除final和static修饰的属性
                if(Modifier.isFinal(modifer)||Modifier.isStatic(modifer)) continue;
                String fieldName = f.getName();
                Method method = null;
                try{
                    String prefix = SET_PREFIX;
                    String fName = fieldName.substring(0,1).toUpperCase()+fieldName.substring(1);
                    method = tClass.getMethod(prefix+fName,getClass(f.getGenericType().getTypeName()));
                    if(null == method) continue;
                    method.invoke(t,map.get(fieldName));
                }catch (Exception e){

                }
            }
            tClass = tClass.getSuperclass();
        }
        return t;
    }

    private static Class<?> getClass(String typeName) throws ClassNotFoundException {
        Class<?> clazz;
        switch (typeName){
            case "byte":
                clazz = byte.class;
                break;
            case "short":
                clazz = short.class;
                break;
            case "int":
                clazz = int.class;
                break;
            case "long":
                clazz = long.class;
                break;
            case "float":
                clazz = float.class;
                break;
            case "boolean":
                clazz = boolean.class;
                break;
            case "double":
                clazz = double.class;
                break;
            case "char":
                clazz = char.class;
                break;
            default:
                clazz = Class.forName(typeName);
        }
        return clazz;
    }

}

