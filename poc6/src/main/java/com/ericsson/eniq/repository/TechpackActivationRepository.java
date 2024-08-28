package com.ericsson.eniq.repository;
	

import org.springframework.data.jpa.repository.JpaRepository;

import com.ericsson.eniq.model.TechpackActivation;


public interface TechpackActivationRepository extends JpaRepository<TechpackActivation, Long> {
  
  TechpackActivation findByTpName(String tpName);
   

}
