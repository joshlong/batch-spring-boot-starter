package com.joshlong.batch.remotechunking;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

@Target({ElementType.FIELD, ElementType.METHOD,
        ElementType.PARAMETER,
        ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@Qualifier
public @interface ChunkingItemWriter {

    @AliasFor(annotation = Qualifier.class)
    String value() default "";

}
