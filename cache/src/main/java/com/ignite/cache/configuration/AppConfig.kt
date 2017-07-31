package com.ignite.cache.configuration

import org.apache.ignite.Ignite
import org.apache.ignite.IgniteSpring
import org.apache.ignite.cache.spring.SpringCacheManager
import org.apache.ignite.configuration.IgniteConfiguration
import org.springframework.cache.CacheManager
import org.springframework.cache.annotation.EnableCaching
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

/**
 * Created by GrIfOn on 31.07.2017.
 */
@Configuration
@ComponentScan(basePackages = arrayOf("com.ignite.cache"))
@EnableCaching
open class AppConfig {

    @Bean
    open fun cacheManager(cfg: IgniteConfiguration): CacheManager {
        val manager = SpringCacheManager()

        manager.configuration = cfg

        return manager
    }

    @Bean
    open fun igniteCfg(): IgniteConfiguration {
        val cfg = IgniteConfiguration()

        cfg.igniteInstanceName = "igniteCache"

        return cfg
    }
}