package com.ericsson.eniq.repository;
	

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.ericsson.eniq.model.FeatureActivation;


public interface FeatureActivationRepository extends JpaRepository<FeatureActivation, Long> {
  
  List<FeatureActivation> findByStatus(String status);
  FeatureActivation findByFeatureDesc(String featureDesc);
  

}
