package com.ignite.cache.configuration

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories
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
@EnableJpaRepositories(basePackages = arrayOf("com.ignite.cache.model.repositories.springdatarepository"))
@EnableIgniteRepositories(basePackages = arrayOf("com.ignite.cache.model.repositories.igniterepository"))
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
                put("database.dialet", "org.hibernate.dialect.PostgreSQL95Dialect")
                put("database.globally_quoted_identifiers", "false")
                put("database.enable_lazy_load_no_trans", "true")
                put("database.show_sql", "true")
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