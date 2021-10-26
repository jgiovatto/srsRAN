/**
 * Copyright 2013-2021 Software Radio Systems Limited
 *
 * This file is part of srsRAN.
 *
 * srsRAN is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * srsRAN is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * A copy of the GNU Affero General Public License can be found in
 * the LICENSE file in the top-level directory of this distribution
 * and at http://www.gnu.org/licenses/.
 *
 */

#include "srsenb/hdr/stack/rrc/nr/cell_asn1_config.h"

using namespace asn1::rrc_nr;

#define HANDLE_ERROR(ret)                                                                                              \
  if ((ret) != SRSRAN_SUCCESS) {                                                                                       \
    return SRSRAN_ERROR;                                                                                               \
  }

namespace srsenb {

srslog::basic_logger& get_logger(const rrc_nr_cfg_t& cfg)
{
  static srslog::basic_logger& log_obj = srslog::fetch_basic_logger(cfg.log_name);
  return log_obj;
}

/// Fill list of CSI-ReportConfig with gNB config
int fill_csi_report_from_enb_cfg(const rrc_nr_cfg_t& cfg, csi_meas_cfg_s& csi_meas_cfg)
{
  csi_meas_cfg.csi_report_cfg_to_add_mod_list_present = true;
  csi_meas_cfg.csi_report_cfg_to_add_mod_list.resize(1);

  auto& csi_report                               = csi_meas_cfg.csi_report_cfg_to_add_mod_list[0];
  csi_report.report_cfg_id                       = 0;
  csi_report.res_for_ch_meas                     = 0;
  csi_report.csi_im_res_for_interference_present = true;
  csi_report.csi_im_res_for_interference         = 1;
  csi_report.report_cfg_type.set_periodic();
  csi_report.report_cfg_type.periodic().report_slot_cfg.set_slots80();
  csi_report.report_cfg_type.periodic().pucch_csi_res_list.resize(1);
  csi_report.report_cfg_type.periodic().pucch_csi_res_list[0].ul_bw_part_id = 0;
  csi_report.report_cfg_type.periodic().pucch_csi_res_list[0].pucch_res     = 1; // was 17 in orig PCAP
  csi_report.report_quant.set_cri_ri_pmi_cqi();
  // Report freq config (optional)
  csi_report.report_freq_cfg_present                = true;
  csi_report.report_freq_cfg.cqi_format_ind_present = true;
  csi_report.report_freq_cfg.cqi_format_ind =
      asn1::rrc_nr::csi_report_cfg_s::report_freq_cfg_s_::cqi_format_ind_opts::wideband_cqi;
  csi_report.time_restrict_for_ch_meass = asn1::rrc_nr::csi_report_cfg_s::time_restrict_for_ch_meass_opts::not_cfgured;
  csi_report.time_restrict_for_interference_meass =
      asn1::rrc_nr::csi_report_cfg_s::time_restrict_for_interference_meass_opts::not_cfgured;
  csi_report.group_based_beam_report.set_disabled();
  // Skip CQI table (optional)
  csi_report.cqi_table_present = true;
  csi_report.cqi_table         = asn1::rrc_nr::csi_report_cfg_s::cqi_table_opts::table2;
  csi_report.subband_size      = asn1::rrc_nr::csi_report_cfg_s::subband_size_opts::value1;

  if (cfg.cell_list[0].duplex_mode == SRSRAN_DUPLEX_MODE_FDD) {
    csi_report.report_cfg_type.periodic().report_slot_cfg.slots80() = 5;
  } else {
    csi_report.report_cfg_type.periodic().report_slot_cfg.slots80() = 7;
  }

  return SRSRAN_SUCCESS;
}

/// Fill lists of NZP-CSI-RS-Resource and NZP-CSI-RS-ResourceSet with gNB config
void fill_nzp_csi_rs_from_enb_cfg(const rrc_nr_cfg_t& cfg, csi_meas_cfg_s& csi_meas_cfg)
{
  csi_meas_cfg.nzp_csi_rs_res_to_add_mod_list_present = true;
  if (cfg.cell_list[0].duplex_mode == SRSRAN_DUPLEX_MODE_FDD) {
    csi_meas_cfg.nzp_csi_rs_res_to_add_mod_list.resize(5);
    auto& nzp_csi_res = csi_meas_cfg.nzp_csi_rs_res_to_add_mod_list;
    // item 0
    nzp_csi_res[0].nzp_csi_rs_res_id = 0;
    nzp_csi_res[0].res_map.freq_domain_alloc.set_row2();
    nzp_csi_res[0].res_map.freq_domain_alloc.row2().from_number(0b100000000000);
    nzp_csi_res[0].res_map.nrof_ports                       = asn1::rrc_nr::csi_rs_res_map_s::nrof_ports_opts::p1;
    nzp_csi_res[0].res_map.first_ofdm_symbol_in_time_domain = 4;
    nzp_csi_res[0].res_map.cdm_type                         = asn1::rrc_nr::csi_rs_res_map_s::cdm_type_opts::no_cdm;
    nzp_csi_res[0].res_map.density.set_one();
    nzp_csi_res[0].res_map.freq_band.start_rb = 0;
    nzp_csi_res[0].res_map.freq_band.nrof_rbs = 52;
    nzp_csi_res[0].pwr_ctrl_offset            = 0;
    // Skip pwr_ctrl_offset_ss_present
    nzp_csi_res[0].scrambling_id                  = cfg.cell_list[0].phy_cell.cell_id;
    nzp_csi_res[0].periodicity_and_offset_present = true;
    nzp_csi_res[0].periodicity_and_offset.set_slots80();
    nzp_csi_res[0].periodicity_and_offset.slots80() = 1;
    // optional
    nzp_csi_res[0].qcl_info_periodic_csi_rs_present = true;
    nzp_csi_res[0].qcl_info_periodic_csi_rs         = 0;
    // item 1
    nzp_csi_res[1]                   = nzp_csi_res[0];
    nzp_csi_res[1].nzp_csi_rs_res_id = 1;
    nzp_csi_res[1].res_map.freq_domain_alloc.set_row1();
    nzp_csi_res[1].res_map.freq_domain_alloc.row1().from_number(0b0001);
    nzp_csi_res[1].res_map.first_ofdm_symbol_in_time_domain = 4;
    nzp_csi_res[1].res_map.density.set_three();
    nzp_csi_res[1].periodicity_and_offset.set_slots40();
    nzp_csi_res[1].periodicity_and_offset.slots40() = 11;
    // item 2
    nzp_csi_res[2]                                          = nzp_csi_res[1];
    nzp_csi_res[2].nzp_csi_rs_res_id                        = 2;
    nzp_csi_res[2].res_map.first_ofdm_symbol_in_time_domain = 8;
    nzp_csi_res[2].periodicity_and_offset.set_slots40();
    nzp_csi_res[2].periodicity_and_offset.slots40() = 11;
    // item 3
    nzp_csi_res[3]                                          = nzp_csi_res[1];
    nzp_csi_res[3].nzp_csi_rs_res_id                        = 3;
    nzp_csi_res[3].res_map.first_ofdm_symbol_in_time_domain = 4;
    nzp_csi_res[3].periodicity_and_offset.set_slots40();
    nzp_csi_res[3].periodicity_and_offset.slots40() = 12;
    // item 4
    nzp_csi_res[4]                                          = nzp_csi_res[1];
    nzp_csi_res[4].nzp_csi_rs_res_id                        = 4;
    nzp_csi_res[4].res_map.first_ofdm_symbol_in_time_domain = 8;
    nzp_csi_res[4].periodicity_and_offset.set_slots40();
    nzp_csi_res[4].periodicity_and_offset.slots40() = 12;
  } else {
    csi_meas_cfg.nzp_csi_rs_res_to_add_mod_list.resize(1);
    auto& nzp_csi_res                = csi_meas_cfg.nzp_csi_rs_res_to_add_mod_list;
    nzp_csi_res[0].nzp_csi_rs_res_id = 0;
    nzp_csi_res[0].res_map.freq_domain_alloc.set_row2();
    nzp_csi_res[0].res_map.freq_domain_alloc.row2().from_number(0b100000000000);
    nzp_csi_res[0].res_map.nrof_ports                       = asn1::rrc_nr::csi_rs_res_map_s::nrof_ports_opts::p1;
    nzp_csi_res[0].res_map.first_ofdm_symbol_in_time_domain = 4;
    nzp_csi_res[0].res_map.cdm_type                         = asn1::rrc_nr::csi_rs_res_map_s::cdm_type_opts::no_cdm;
    nzp_csi_res[0].res_map.density.set_one();
    nzp_csi_res[0].res_map.freq_band.start_rb = 0;
    nzp_csi_res[0].res_map.freq_band.nrof_rbs = 52;
    nzp_csi_res[0].pwr_ctrl_offset            = 0;
    // Skip pwr_ctrl_offset_ss_present
    nzp_csi_res[0].periodicity_and_offset_present       = true;
    nzp_csi_res[0].periodicity_and_offset.set_slots80() = 0;
    // optional
    nzp_csi_res[0].qcl_info_periodic_csi_rs_present = true;
    nzp_csi_res[0].qcl_info_periodic_csi_rs         = 0;
  }

  // Fill NZP-CSI Resource Sets
  csi_meas_cfg.nzp_csi_rs_res_set_to_add_mod_list_present = true;
  if (cfg.cell_list[0].duplex_mode == SRSRAN_DUPLEX_MODE_FDD) {
    csi_meas_cfg.nzp_csi_rs_res_set_to_add_mod_list.resize(2);
    auto& nzp_csi_res_set = csi_meas_cfg.nzp_csi_rs_res_set_to_add_mod_list;
    // item 0
    nzp_csi_res_set[0].nzp_csi_res_set_id = 0;
    nzp_csi_res_set[0].nzp_csi_rs_res.resize(1);
    nzp_csi_res_set[0].nzp_csi_rs_res[0] = 0;
    // item 1
    nzp_csi_res_set[1].nzp_csi_res_set_id = 1;
    nzp_csi_res_set[1].nzp_csi_rs_res.resize(4);
    nzp_csi_res_set[1].nzp_csi_rs_res[0] = 1;
    nzp_csi_res_set[1].nzp_csi_rs_res[1] = 2;
    nzp_csi_res_set[1].nzp_csi_rs_res[2] = 3;
    nzp_csi_res_set[1].nzp_csi_rs_res[3] = 4;
    //    // Skip TRS info
  } else {
    csi_meas_cfg.nzp_csi_rs_res_set_to_add_mod_list.resize(1);
    auto& nzp_csi_res_set                 = csi_meas_cfg.nzp_csi_rs_res_set_to_add_mod_list;
    nzp_csi_res_set[0].nzp_csi_res_set_id = 0;
    nzp_csi_res_set[0].nzp_csi_rs_res.resize(1);
    nzp_csi_res_set[0].nzp_csi_rs_res[0] = 0;
    // Skip TRS info
  }
}

/// Fill csi-ResoureConfigToAddModList
void fill_csi_resource_cfg_to_add(const rrc_nr_cfg_t& cfg, csi_meas_cfg_s& csi_meas_cfg)
{
  if (cfg.cell_list[0].duplex_mode == SRSRAN_DUPLEX_MODE_FDD) {
    csi_meas_cfg.csi_res_cfg_to_add_mod_list_present = true;
    csi_meas_cfg.csi_res_cfg_to_add_mod_list.resize(3);

    csi_meas_cfg.csi_res_cfg_to_add_mod_list[0].csi_res_cfg_id = 0;
    auto& nzp = csi_meas_cfg.csi_res_cfg_to_add_mod_list[0].csi_rs_res_set_list.set_nzp_csi_rs_ssb();
    nzp.nzp_csi_rs_res_set_list_present = true;
    nzp.nzp_csi_rs_res_set_list.push_back(0);
    csi_meas_cfg.csi_res_cfg_to_add_mod_list[0].bwp_id         = 0;
    csi_meas_cfg.csi_res_cfg_to_add_mod_list[0].res_type.value = csi_res_cfg_s::res_type_opts::periodic;

    csi_meas_cfg.csi_res_cfg_to_add_mod_list[1].csi_res_cfg_id = 1;
    auto& imres = csi_meas_cfg.csi_res_cfg_to_add_mod_list[1].csi_rs_res_set_list.set_csi_im_res_set_list();
    imres.push_back(0);
    csi_meas_cfg.csi_res_cfg_to_add_mod_list[1].bwp_id         = 0;
    csi_meas_cfg.csi_res_cfg_to_add_mod_list[1].res_type.value = csi_res_cfg_s::res_type_opts::periodic;

    csi_meas_cfg.csi_res_cfg_to_add_mod_list[2].csi_res_cfg_id = 2;
    auto& nzp2 = csi_meas_cfg.csi_res_cfg_to_add_mod_list[2].csi_rs_res_set_list.set_nzp_csi_rs_ssb();
    nzp2.nzp_csi_rs_res_set_list_present = true;
    nzp2.nzp_csi_rs_res_set_list.push_back(1);
    csi_meas_cfg.csi_res_cfg_to_add_mod_list[2].bwp_id         = 0;
    csi_meas_cfg.csi_res_cfg_to_add_mod_list[2].res_type.value = csi_res_cfg_s::res_type_opts::periodic;
  }
}

/// Fill CSI-MeasConfig with gNB config
int fill_csi_meas_from_enb_cfg(const rrc_nr_cfg_t& cfg, csi_meas_cfg_s& csi_meas_cfg)
{
  //  // Fill CSI Report
  //  if (fill_csi_report_from_enb_cfg(cfg, csi_meas_cfg) != SRSRAN_SUCCESS) {
  //    get_logger(cfg).error("Failed to configure eNB CSI Report");
  //    return SRSRAN_ERROR;
  //  }

  // Fill CSI resource config
  fill_csi_resource_cfg_to_add(cfg, csi_meas_cfg);

  // Fill NZP-CSI Resources
  fill_nzp_csi_rs_from_enb_cfg(cfg, csi_meas_cfg);

  // CSI IM config
  // TODO: add csi im config

  // CSI resource config
  // TODO: add csi resource config

  return SRSRAN_SUCCESS;
}

void fill_init_dl_bwp(const rrc_nr_cfg_t& cfg, bwp_dl_ded_s& bwp)
{
  bwp.pdcch_cfg_present                                = true;
  auto& pdcch_ded_setup                                = bwp.pdcch_cfg.set_setup();
  pdcch_ded_setup.ctrl_res_set_to_add_mod_list_present = true;
  pdcch_ded_setup.ctrl_res_set_to_add_mod_list.resize(1);
  pdcch_ded_setup.ctrl_res_set_to_add_mod_list[0].ctrl_res_set_id = 2;
  pdcch_ded_setup.ctrl_res_set_to_add_mod_list[0].freq_domain_res.from_number(
      0b111111110000000000000000000000000000000000000);
  pdcch_ded_setup.ctrl_res_set_to_add_mod_list[0].dur = 1;
  pdcch_ded_setup.ctrl_res_set_to_add_mod_list[0].cce_reg_map_type.set_non_interleaved();
  pdcch_ded_setup.ctrl_res_set_to_add_mod_list[0].precoder_granularity =
      asn1::rrc_nr::ctrl_res_set_s::precoder_granularity_opts::same_as_reg_bundle;

  // search spaces
  pdcch_ded_setup.search_spaces_to_add_mod_list_present = true;
  pdcch_ded_setup.search_spaces_to_add_mod_list.resize(1);
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].search_space_id                                = 2;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].ctrl_res_set_id_present                        = true;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].ctrl_res_set_id                                = 2;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].monitoring_slot_periodicity_and_offset_present = true;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].monitoring_slot_periodicity_and_offset.set_sl1();
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].monitoring_symbols_within_slot_present = true;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].monitoring_symbols_within_slot.from_number(0b10000000000000);
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].nrof_candidates_present = true;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].nrof_candidates.aggregation_level1 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level1_opts::n0;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].nrof_candidates.aggregation_level2 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level2_opts::n2;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].nrof_candidates.aggregation_level4 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level4_opts::n2;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].nrof_candidates.aggregation_level8 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level8_opts::n0;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].nrof_candidates.aggregation_level16 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level16_opts::n0;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].search_space_type_present = true;
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].search_space_type.set_ue_specific();
  pdcch_ded_setup.search_spaces_to_add_mod_list[0].search_space_type.ue_specific().dci_formats = asn1::rrc_nr::
      search_space_s::search_space_type_c_::ue_specific_s_::dci_formats_opts::formats0_minus0_and_minus1_minus0;
}

/// Fill ServingCellConfig with gNB config
int fill_serv_cell_from_enb_cfg(const rrc_nr_cfg_t& cfg, serving_cell_cfg_s& serv_cell)
{
  serv_cell.csi_meas_cfg_present = true;
  HANDLE_ERROR(fill_csi_meas_from_enb_cfg(cfg, serv_cell.csi_meas_cfg.set_setup()));

  serv_cell.init_dl_bwp_present = true;
  fill_init_dl_bwp(cfg, serv_cell.init_dl_bwp);

  // TODO: remaining fields

  return SRSRAN_SUCCESS;
}

void fill_dl_cfg_common_from_enb_cfg(const rrc_nr_cfg_t& cfg, uint32_t cc, dl_cfg_common_s& dl_cfg_common)
{
  dl_cfg_common.init_dl_bwp_present                  = true;
  dl_cfg_common.init_dl_bwp.pdcch_cfg_common_present = true;

  auto& pdcch_cfg_setup                               = dl_cfg_common.init_dl_bwp.pdcch_cfg_common.set_setup();
  pdcch_cfg_setup.common_ctrl_res_set_present         = true;
  pdcch_cfg_setup.common_ctrl_res_set.ctrl_res_set_id = 1;
  pdcch_cfg_setup.common_ctrl_res_set.freq_domain_res.from_number(0b111111110000000000000000000000000000000000000);
  pdcch_cfg_setup.common_ctrl_res_set.dur = 1;
  pdcch_cfg_setup.common_ctrl_res_set.cce_reg_map_type.set_non_interleaved();
  pdcch_cfg_setup.common_ctrl_res_set.precoder_granularity =
      asn1::rrc_nr::ctrl_res_set_s::precoder_granularity_opts::same_as_reg_bundle;

  // common search space list
  pdcch_cfg_setup.common_search_space_list_present = true;
  pdcch_cfg_setup.common_search_space_list.resize(1);
  pdcch_cfg_setup.common_search_space_list[0].search_space_id           = 1;
  pdcch_cfg_setup.common_search_space_list[0].ctrl_res_set_id_present   = true;
  pdcch_cfg_setup.common_search_space_list[0].ctrl_res_set_id           = 1;
  pdcch_cfg_setup.common_search_space_list[0].search_space_type_present = true;
  pdcch_cfg_setup.common_search_space_list[0].search_space_type.set_common();
  pdcch_cfg_setup.common_search_space_list[0].search_space_type.common().dci_format0_minus0_and_format1_minus0_present =
      true;
  pdcch_cfg_setup.common_search_space_list[0].nrof_candidates_present = true;
  pdcch_cfg_setup.common_search_space_list[0].nrof_candidates.aggregation_level1 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level1_opts::n1;
  pdcch_cfg_setup.common_search_space_list[0].nrof_candidates.aggregation_level2 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level2_opts::n1;
  pdcch_cfg_setup.common_search_space_list[0].nrof_candidates.aggregation_level4 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level4_opts::n1;
  pdcch_cfg_setup.common_search_space_list[0].nrof_candidates.aggregation_level8 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level8_opts::n0;
  pdcch_cfg_setup.common_search_space_list[0].nrof_candidates.aggregation_level16 =
      asn1::rrc_nr::search_space_s::nrof_candidates_s_::aggregation_level16_opts::n0;
  pdcch_cfg_setup.common_search_space_list[0].monitoring_slot_periodicity_and_offset_present = true;
  pdcch_cfg_setup.common_search_space_list[0].monitoring_slot_periodicity_and_offset.set_sl1();
  pdcch_cfg_setup.common_search_space_list[0].monitoring_symbols_within_slot_present = true;
  pdcch_cfg_setup.common_search_space_list[0].monitoring_symbols_within_slot.from_number(0b10000000000000);
  pdcch_cfg_setup.ra_search_space_present = true;
  pdcch_cfg_setup.ra_search_space         = 1;

  if (cfg.cell_list[cc].duplex_mode == SRSRAN_DUPLEX_MODE_TDD) {
    pdcch_cfg_setup.ext = false;
  }
}

/// Fill ServingCellConfigCommon with gNB config
int fill_serv_cell_common_from_enb_cfg(const rrc_nr_cfg_t& cfg, uint32_t cc, serving_cell_cfg_common_s& serv_common)
{
  auto& cell_cfg = cfg.cell_list.at(cc);

  serv_common.ss_pbch_block_pwr               = 20;
  serv_common.n_timing_advance_offset_present = true;
  serv_common.n_timing_advance_offset = asn1::rrc_nr::serving_cell_cfg_common_s::n_timing_advance_offset_opts::n0;
  serv_common.n_timing_advance_offset_present = true;
  serv_common.dmrs_type_a_position = asn1::rrc_nr::serving_cell_cfg_common_s::dmrs_type_a_position_opts::pos2;

  serv_common.pci_present = true;
  serv_common.pci         = cell_cfg.phy_cell.carrier.pci;

  serv_common.ssb_periodicity_serving_cell_present = true;
  if (not asn1::number_to_enum(serv_common.ssb_periodicity_serving_cell, cell_cfg.ssb_cfg.periodicity_ms)) {
    get_logger(cfg).error("Config Error: Invalid SSB periodicity = %d\n", cell_cfg.ssb_cfg.periodicity_ms);
    return SRSRAN_ERROR;
  }

  // Fill SSB config
  serv_common.ssb_positions_in_burst_present = true;
  if (cfg.cell_list[cc].duplex_mode == SRSRAN_DUPLEX_MODE_FDD) {
    serv_common.ssb_positions_in_burst.set_short_bitmap().from_number(0b1000);
  } else {
    serv_common.ssb_positions_in_burst.set_medium_bitmap().from_number(0b10000000);
  }

  // Set SSB SCS
  serv_common.ssb_subcarrier_spacing_present = true;
  if (cfg.cell_list[cc].duplex_mode == SRSRAN_DUPLEX_MODE_FDD) {
    serv_common.ssb_subcarrier_spacing = subcarrier_spacing_opts::khz15;
  } else {
    serv_common.ssb_subcarrier_spacing = subcarrier_spacing_opts::khz30;
  }

  if (cfg.cell_list[cc].duplex_mode == SRSRAN_DUPLEX_MODE_TDD) {
    // TDD UL-DL config
    serv_common.tdd_ul_dl_cfg_common_present = true;
    auto& tdd_config                         = serv_common.tdd_ul_dl_cfg_common;
    tdd_config.ref_subcarrier_spacing        = subcarrier_spacing_e::khz15;
    tdd_config.pattern1.dl_ul_tx_periodicity = asn1::rrc_nr::tdd_ul_dl_pattern_s::dl_ul_tx_periodicity_opts::ms10;
    tdd_config.pattern1.nrof_dl_slots        = 6;
    tdd_config.pattern1.nrof_dl_symbols      = 0;
    tdd_config.pattern1.nrof_ul_slots        = 4;
    tdd_config.pattern1.nrof_ul_symbols      = 0;
  }

  serv_common.dl_cfg_common_present = true;
  fill_dl_cfg_common_from_enb_cfg(cfg, cc, serv_common.dl_cfg_common);

  return SRSRAN_SUCCESS;
}

/// Fill reconfigurationWithSync with gNB config
int fill_recfg_with_sync_from_enb_cfg(const rrc_nr_cfg_t& cfg, uint32_t cc, recfg_with_sync_s& sync)
{
  sync.sp_cell_cfg_common_present = true;
  HANDLE_ERROR(fill_serv_cell_common_from_enb_cfg(cfg, cc, sync.sp_cell_cfg_common));

  return SRSRAN_SUCCESS;
}

/// Fill spCellConfig with gNB config
int fill_sp_cell_cfg_from_enb_cfg(const rrc_nr_cfg_t& cfg, uint32_t cc, sp_cell_cfg_s& sp_cell)
{
  sp_cell.recfg_with_sync_present = true;
  HANDLE_ERROR(fill_recfg_with_sync_from_enb_cfg(cfg, cc, sp_cell.recfg_with_sync));

  sp_cell.sp_cell_cfg_ded_present = true;
  HANDLE_ERROR(fill_serv_cell_from_enb_cfg(cfg, sp_cell.sp_cell_cfg_ded));

  return SRSRAN_SUCCESS;
}

} // namespace srsenb