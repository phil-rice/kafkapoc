package com.hcltech.rmg.interfaces.repository;

import java.lang.reflect.Field;

/** A marker interface for pipeline repositories. Because of the limitations forced on us by flink these repositories are
 * statics. So this must have a static variable called instance which is the singleton instance of the repository.
 * */
public interface IPipelineRepository<From,To> {
    PipelineDetails<From,To> pipelineDetails();


    /**
     * Look up a repository by class name, fetch its static `instance` field,
     * and return it as an IPipelineRepository.
     *
     * @param className fully qualified class name
     * @return the repository instance
     * @throws IllegalArgumentException if the class/field/type checks fail
     */
    static <From,To>IPipelineRepository<From,To> load(String className) {
        try {
            Class<?> clazz = Class.forName(className);

            Field instanceField = clazz.getField("instance"); // must be public static
            Object value = instanceField.get(null);           // static → null target

            if (!(value instanceof IPipelineRepository<?, ?> repo)) {
                throw new IllegalArgumentException(
                        "Class " + className + ".instance is not an IPipelineRepository"
                );
            }
            return (IPipelineRepository<From, To>) repo;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Repository class not found: " + className, e);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Class " + className + " has no public static 'instance' field", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot access 'instance' field on " + className, e);
        }
    }
}
