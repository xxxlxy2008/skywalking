/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance;

import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.AbstractClassEnhancePluginDefine;
import org.apache.skywalking.apm.agent.core.plugin.EnhanceContext;
import org.apache.skywalking.apm.agent.core.plugin.PluginException;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.*;
import org.apache.skywalking.apm.util.StringUtil;

import static net.bytebuddy.jar.asm.Opcodes.ACC_PRIVATE;
import static net.bytebuddy.jar.asm.Opcodes.ACC_VOLATILE;
import static net.bytebuddy.matcher.ElementMatchers.isStatic;
import static net.bytebuddy.matcher.ElementMatchers.not;

/**
 * This class controls all enhance operations, including enhance constructors, instance methods and static methods. All
 * the enhances base on three types interceptor point: {@link ConstructorInterceptPoint}, {@link
 * InstanceMethodsInterceptPoint} and {@link StaticMethodsInterceptPoint} If plugin is going to enhance constructors,
 * instance methods, or both, {@link ClassEnhancePluginDefine} will add a field of {@link
 * Object} type.
 *
 * @author wusheng
 */
public abstract class ClassEnhancePluginDefine extends AbstractClassEnhancePluginDefine {
    private static final ILog logger = LogManager.getLogger(ClassEnhancePluginDefine.class);

    /**
     * New field name.
     */
    public static final String CONTEXT_ATTR_NAME = "_$EnhancedClassField_ws";

    /**
     * Begin to define how to enhance class.
     * After invoke this method, only means definition is finished.
     *
     * @param typeDescription target class description
     * @param newClassBuilder byte-buddy's builder to manipulate class bytecode.
     * @return new byte-buddy's builder for further manipulation.
     */
    @Override
    protected DynamicType.Builder<?> enhance(TypeDescription typeDescription,
                                             DynamicType.Builder<?> newClassBuilder, ClassLoader classLoader,
                                             EnhanceContext context) throws PluginException {
        newClassBuilder = this.enhanceClass(typeDescription, newClassBuilder, classLoader); // 增强static方法

        newClassBuilder = this.enhanceInstance(typeDescription, newClassBuilder, classLoader, context); // 增强构造方法和实例方法

        return newClassBuilder;
    }

    /**
     * Enhance a class to intercept constructors and class instance methods.
     *
     * @param typeDescription target class description
     * @param newClassBuilder byte-buddy's builder to manipulate class bytecode.
     * @return new byte-buddy's builder for further manipulation.
     */
    private DynamicType.Builder<?> enhanceInstance(TypeDescription typeDescription,
        DynamicType.Builder<?> newClassBuilder, ClassLoader classLoader,
        EnhanceContext context) throws PluginException {
        ConstructorInterceptPoint[] constructorInterceptPoints = getConstructorsInterceptPoints();
        InstanceMethodsInterceptPoint[] instanceMethodsInterceptPoints = getInstanceMethodsInterceptPoints();
        String enhanceOriginClassName = typeDescription.getTypeName();
        boolean existedConstructorInterceptPoint = false;
        if (constructorInterceptPoints != null && constructorInterceptPoints.length > 0) {
            existedConstructorInterceptPoint = true;
        }
        boolean existedMethodsInterceptPoints = false;
        if (instanceMethodsInterceptPoints != null && instanceMethodsInterceptPoints.length > 0) {
            existedMethodsInterceptPoints = true;
        }

        /**
         * nothing need to be enhanced in class instance, maybe need enhance static methods.
         */
        if (!existedConstructorInterceptPoint && !existedMethodsInterceptPoints) {
            return newClassBuilder;
        }

        /**
         * Manipulate class source code.<br/>
         *
         * new class need:<br/>
         * 1.Add field, name {@link #CONTEXT_ATTR_NAME}.
         * 2.Add a field accessor for this field.
         *
         * And make sure the source codes manipulation only occurs once.
         *
         */
        if (!context.isObjectExtended()) { // 检测是否已经被增强过了,这里先添加一个字段以及getter/setter方法
            newClassBuilder = newClassBuilder.defineField(CONTEXT_ATTR_NAME, Object.class, ACC_PRIVATE | ACC_VOLATILE)
                .implement(EnhancedInstance.class) // 实现EnhancedInstance接口
                .intercept(FieldAccessor.ofField(CONTEXT_ATTR_NAME));// 用读写CONTEXT_ATTR_NAME字段的方式实现EnhancedInstance接口
            context.extendObjectCompleted();
        }

        /**
         * 2. enhance constructors
         */
        if (existedConstructorInterceptPoint) {// 遍历 ConstructorInterceptPoint 数组，逐个增强 ConstructorInterceptPoint 对应的构造方法
            for (ConstructorInterceptPoint constructorInterceptPoint : constructorInterceptPoints) { // SuperMethodCall.INSTANCE就是调一下super()
                newClassBuilder = newClassBuilder.constructor(constructorInterceptPoint.getConstructorMatcher()).intercept(SuperMethodCall.INSTANCE
                    .andThen(MethodDelegation.withDefaultConfiguration() // 调用完super()，调插件
                        .to(new ConstructorInter(constructorInterceptPoint.getConstructorInterceptor(), classLoader))
                    )
                );
            }
        }

        /**
         * 3. enhance instance methods
         */
        if (existedMethodsInterceptPoints) { // 遍历 InstanceMethodsInterceptPoint 数组，逐个增强 InstanceMethodsInterceptPoint 对应的静态方法。
            for (InstanceMethodsInterceptPoint instanceMethodsInterceptPoint : instanceMethodsInterceptPoints) {
                String interceptor = instanceMethodsInterceptPoint.getMethodsInterceptor();
                if (StringUtil.isEmpty(interceptor)) {
                    throw new EnhanceException("no InstanceMethodsAroundInterceptor define to enhance class " + enhanceOriginClassName);
                }
                ElementMatcher.Junction<MethodDescription> junction = not(isStatic()).and(instanceMethodsInterceptPoint.getMethodsMatcher()); // 过滤方法
                if (instanceMethodsInterceptPoint instanceof DeclaredInstanceMethodsInterceptPoint) {
                    junction = junction.and(ElementMatchers.<MethodDescription>isDeclaredBy(typeDescription)); // 过滤条件:目标类中定义了这个方法
                }
                if (instanceMethodsInterceptPoint.isOverrideArgs()) {
                    newClassBuilder =
                        newClassBuilder.method(junction) // method方法就是用来过滤方法的
                            .intercept(
                                MethodDelegation.withDefaultConfiguration()
                                    .withBinders( // 要用@Morph注解之前，需要通过Morph.Binder绑定一下
                                        Morph.Binder.install(OverrideCallable.class)
                                    )
                                    .to(new InstMethodsInterWithOverrideArgs(interceptor, classLoader))
                            );
                } else {
                    newClassBuilder =
                        newClassBuilder.method(junction)
                            .intercept(
                                MethodDelegation.withDefaultConfiguration()
                                    .to(new InstMethodsInter(interceptor, classLoader))
                            );
                }
            }
        }

        return newClassBuilder;
    }

    /**
     * Constructor methods intercept point. See {@link ConstructorInterceptPoint}
     *
     * @return collections of {@link ConstructorInterceptPoint}
     */
    protected abstract ConstructorInterceptPoint[] getConstructorsInterceptPoints();

    /**
     * Instance methods intercept point. See {@link InstanceMethodsInterceptPoint}
     *
     * @return collections of {@link InstanceMethodsInterceptPoint}
     */
    protected abstract InstanceMethodsInterceptPoint[] getInstanceMethodsInterceptPoints();

    /**
     * Enhance a class to intercept class static methods.
     *
     * @param typeDescription target class description
     * @param newClassBuilder byte-buddy's builder to manipulate class bytecode.
     * @return new byte-buddy's builder for further manipulation.
     */
    private DynamicType.Builder<?> enhanceClass(TypeDescription typeDescription,
        DynamicType.Builder<?> newClassBuilder, ClassLoader classLoader) throws PluginException {
        StaticMethodsInterceptPoint[] staticMethodsInterceptPoints = getStaticMethodsInterceptPoints();
        String enhanceOriginClassName = typeDescription.getTypeName();
        if (staticMethodsInterceptPoints == null || staticMethodsInterceptPoints.length == 0) {
            return newClassBuilder;
        }

        for (StaticMethodsInterceptPoint staticMethodsInterceptPoint : staticMethodsInterceptPoints) {
            String interceptor = staticMethodsInterceptPoint.getMethodsInterceptor();
            if (StringUtil.isEmpty(interceptor)) {
                throw new EnhanceException("no StaticMethodsAroundInterceptor define to enhance class " + enhanceOriginClassName);
            }

            if (staticMethodsInterceptPoint.isOverrideArgs()) {
                newClassBuilder = newClassBuilder.method(isStatic().and(staticMethodsInterceptPoint.getMethodsMatcher())) // 过滤方法
                    .intercept(
                        MethodDelegation.withDefaultConfiguration()
                            .withBinders( // 要用Morph注解，也是需要先绑定一下
                                Morph.Binder.install(OverrideCallable.class)
                            )
                            .to(new StaticMethodsInterWithOverrideArgs(interceptor))
                    );
            } else {
                newClassBuilder = newClassBuilder.method(isStatic().and(staticMethodsInterceptPoint.getMethodsMatcher()))
                    .intercept(
                        MethodDelegation.withDefaultConfiguration()
                            .to(new StaticMethodsInter(interceptor))
                    );
            }

        }

        return newClassBuilder;
    }

    /**
     * Static methods intercept point. See {@link StaticMethodsInterceptPoint}
     *
     * @return collections of {@link StaticMethodsInterceptPoint}
     */
    protected abstract StaticMethodsInterceptPoint[] getStaticMethodsInterceptPoints();
}
