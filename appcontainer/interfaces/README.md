# App Container Interfaces Module

## Overview
This module defines the core interfaces and definitions for the RMG application container framework, providing contracts for application configuration and AI aspect definitions.

## Components

### Core Interfaces

#### IAppContainerFactory
- **Purpose**: Factory interface for creating app container instances
- **Location**: `src/main/java/com/hcltech/rmg/appcontainer/interfaces/IAppContainerFactory.java`
- **Usage**: Defines the contract for app container creation
- **Testing**: Caching behavior tested in `IAppContainerFactoryCachingTest.java`

#### AppContainer
- **Purpose**: Main container interface holding application context and configuration
- **Location**: `src/main/java/com/hcltech/rmg/appcontainer/interfaces/AppContainer.java`
- **Usage**: Provides access to application-level resources and configuration

### Definition Classes

#### AppContainerDefn
- **Purpose**: Defines the structure and metadata for an app container
- **Location**: `src/main/java/com/hcltech/rmg/appcontainer/interfaces/AppContainerDefn.java`
- **Usage**: Configuration definition for app containers

#### AiDefn
- **Purpose**: Defines AI aspect configurations and metadata
- **Location**: `src/main/java/com/hcltech/rmg/appcontainer/interfaces/AiDefn.java`
- **Usage**: Specifies AI processing aspects, rules, and behaviors

## Architecture Principles
- **Interface Segregation**: Clean separation between interface and implementation
- **Factory Pattern**: Consistent object creation through factory interfaces
- **Configuration as Code**: Strongly-typed configuration definitions
- **Testability**: Interfaces designed for easy mocking and testing

## Testing
Contract tests are available in `src/test/java/com/hcltech/rmg/appcontainer/interfaces/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module serves as the foundation for the app container framework. Implementations are provided in the `appcontainer/implementation` module, and these interfaces are used throughout the RMG execution framework.

## Design Pattern
This module follows the **Hexagonal Architecture** (Ports and Adapters) pattern:
- Interfaces define **ports** (contracts)
- Implementations (in separate module) provide **adapters**
- Domain logic remains decoupled from infrastructure concerns

