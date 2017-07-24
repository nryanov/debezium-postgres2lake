package com.ignite.cache.configuration

import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.log4j.Logger
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

/**
 * Created by GrIfOn on 23.07.2017.
 */
@Configuration
@ComponentScan(basePackages = arrayOf("com.ignite.cache"))
open class AppConfiguration {
    private val logger: Logger = Logger.getLogger(AppConfiguration::class.java)

    @Bean
    open fun ignite() : Ignite = Ignition.start("ignite-cache.xml")
}