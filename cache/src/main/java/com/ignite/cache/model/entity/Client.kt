package com.ignite.cache.model.entity

import org.hibernate.annotations.GenericGenerator
import org.springframework.cache.annotation.Cacheable
import java.io.Serializable
import javax.persistence.*

/**
 * Created by GrIfOn on 31.07.2017.
 */
@Entity
@Table(name = "client")
@Cacheable("client")
data class Client
(
        @Id
        @Column(name = "id")
        @GeneratedValue(generator = "increment")
        @GenericGenerator(name = "increment", strategy = "increment")
        var id: Long = 0,
        @Column(name = "email", nullable = false, unique = true)
        var email: String = "",
        @Column(name = "login", nullable = false, unique = true)
        var login: String = ""
) : Serializable