package springcache.configuration

import org.springframework.cache.CacheManager
import org.springframework.cache.annotation.EnableCaching
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.cache.ehcache.EhCacheManagerFactoryBean
import org.springframework.cache.ehcache.EhCacheCacheManager



/**
 * Created by GrIfOn on 24.07.2017.
 */
@Configuration
@ComponentScan(basePackages = arrayOf("springcache"))
@EnableCaching
open class AppConfiguration {

    @Bean
    open fun cacheManager(): CacheManager {
        return EhCacheCacheManager(ehCacheCacheManager().`object`)
    }

    @Bean
    open fun ehCacheCacheManager(): EhCacheManagerFactoryBean {
        val factory = EhCacheManagerFactoryBean()
        factory.setConfigLocation(ClassPathResource("ehcache.xml"))
        factory.setShared(true)
        return factory
    }
}