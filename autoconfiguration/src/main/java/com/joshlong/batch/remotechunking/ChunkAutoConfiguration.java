package com.joshlong.batch.remotechunking;

import lombok.extern.slf4j.Slf4j;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.batch.core.*;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.AbstractOwnableSynchronizer;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Configuration
@ImportRuntimeHints(ChunkAutoConfiguration.Hints.class)
@Slf4j
class ChunkAutoConfiguration {

	ChunkAutoConfiguration() {
		log.info("running " + ChunkAutoConfiguration.class.getName());
	}

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			var classLiterals = Set.of(
					// batch
					org.springframework.batch.core.JobParameters.class,
					org.springframework.batch.core.JobParameter.class, ChunkRequest.class, ExitStatus.class,
					StepContribution.class, JobExecution.class, Entity.class, ExecutionContext.class,
					StepExecution.class, ChunkResponse.class, Chunk.class, JobInstance.class,

					// jdk

					java.util.LinkedHashSet.class, java.util.LinkedHashMap.class, java.util.HashSet.class,
					ReentrantLock.class, ConcurrentHashMap.class, AbstractOwnableSynchronizer.class,
					AbstractQueuedSynchronizer.class);
			var classStrings = Set.of(
					// jdk
					"java.time.Ser", "java.util.Collections$SynchronizedSet",
					"java.util.Collections$SynchronizedCollection", "java.util.concurrent.locks.ReentrantLock$Sync",
					"java.util.concurrent.locks.ReentrantLock$FairSync",
					"java.util.concurrent.locks.ReentrantLock$NonfairSync",
					"java.util.concurrent.ConcurrentHashMap$Segment");
			var all = new HashSet<String>();
			all.addAll(classLiterals.stream().map(Class::getName).collect(Collectors.toSet()));
			all.addAll(classStrings);

			var typeReferences = all.stream().map(TypeReference::of).collect(Collectors.toSet());
			typeReferences.forEach(c -> hints.serialization().registerType(c));
			typeReferences.forEach(c -> hints.reflection().registerType(c, MemberCategory.values()));

		}

	}

}
