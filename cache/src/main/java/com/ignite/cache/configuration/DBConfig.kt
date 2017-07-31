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
import java.util.*
import javax.sql.DataSource

/**
 * Created by GrIfOn on 31.07.2017.
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = arrayOf("com.ignite.cache.model.repository"))
open class DBConfig {

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