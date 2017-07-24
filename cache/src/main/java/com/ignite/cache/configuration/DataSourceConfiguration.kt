package com.ignite.cache.configuration

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement
import java.io.Serializable
import java.util.*
import javax.sql.DataSource

/**
 * Created by GrIfOn on 23.07.2017.
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = arrayOf("com.ignite.cache.model.repositories"))
@ComponentScan(basePackages = arrayOf("com.ignite.cache.model"))
open class DataSourceConfiguration : Serializable {

    @Bean
    open fun entityManagerFactory(): LocalContainerEntityManagerFactoryBean {
        var entityManagerFactory: LocalContainerEntityManagerFactoryBean = LocalContainerEntityManagerFactoryBean()

        entityManagerFactory.apply {
            dataSource = dataSource()
            setPackagesToScan("com.ignite.cache.model")

            var vendorAdapter: HibernateJpaVendorAdapter = HibernateJpaVendorAdapter()

            vendorAdapter.apply {
                setGenerateDdl(true)
                setShowSql(true)
            }

            var properties: Properties = Properties()

            properties.apply {
                put("hibernate.dialet", "org.hibernate.dialect.PostgreSQL95Dialect")
                put("hibernate.globally_quoted_identifiers", "false")
                put("hibernate.enable_lazy_load_no_trans", "true")
                put("hibernate.show_sql", "true")
                // CACHE L2 Ignite
                put("hibernate.cache.use_second_level_cache", "true")
                put("hibernate.cache.use_query_cache", "true")
                put("hibernate.cache.region.factory_class", "org.apache.ignite.cache.hibernate.HibernateRegionFactory")
                put("org.apache.ignite.hibernate.ignite_instance_name", "clientGrid")
                put("org.apache.ignite.hibernate.default_access_type", "READ_ONLY")
            }

            jpaVendorAdapter = vendorAdapter
            setJpaProperties(properties)
        }

        return entityManagerFactory
    }

    @Bean
    open fun dataSource(): DataSource {
        var source: ComboPooledDataSource = ComboPooledDataSource()

        source.apply {
            driverClass = "org.postgresql.Driver"
            jdbcUrl = "jdbc:postgresql://localhost:5432/ignite"
            user = "postgres"
            password = "1111"
            acquireIncrement = 5
            idleConnectionTestPeriod = 60
            maxPoolSize = 20
            minPoolSize = 10
            initialPoolSize = 10
        }

        return source
    }

    @Bean
    open fun transactionManager() : PlatformTransactionManager {
        var manager: JpaTransactionManager = JpaTransactionManager()

        manager.apply {
            entityManagerFactory = entityManagerFactory().nativeEntityManagerFactory
        }

        return manager
    }

    @Bean
    open fun exceptionTranslator(): PersistenceExceptionTranslationPostProcessor = PersistenceExceptionTranslationPostProcessor()
}