import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import { DatabricksConnectionProvider } from './databricks.provider';
import knex from 'knex';
import { CASE_WHEN_NOME_INTERNO_CAMPANHA } from '../utils/query';

@Injectable()
export class PlatformDayService {
  constructor(private readonly connectionProvider: DatabricksConnectionProvider) { }
  private knexBuilder = knex({
    client: 'pg',
    useNullAsDefault: true,
    wrapIdentifier: (value, origImpl) => `\`${value}\``
  })

  private aggregateCampaignData(data) {
    return data.reduce((acc, campaign) => {
      const { nomeInternoCampanha, likes, comment, views } = campaign;
      const key = `${nomeInternoCampanha}`;

      if (!acc[key]) {
        acc[key] = {
          nomeInternoCampanha,
          likes: 0,
          comment: 0,
          views: 0
        };
      }

      acc[key].likes += (likes ?? 0);
      acc[key].comment += (comment ?? 0);
      acc[key].views += (views ?? 0);

      return acc;
    }, {});
  }


  async getMetaEngagement(campaignName?: string, startDate?: string, endDate?: string) {
    let query = this.knexBuilder
      .select([
        this.knexBuilder.raw(CASE_WHEN_NOME_INTERNO_CAMPANHA),
        'actions',
        'video_views',
      ])
      .from('main.plataformas.plataformas_meta')
    try {
      if (!!campaignName) {
        query.where('Nome_Interno_Campanha', campaignName)
      }

      if (!!startDate && !!endDate) {
        query.whereBetween('date', [startDate, endDate])
      } else {
        const today = new Date()
        let lastWeek = new Date()
        lastWeek.setDate(today.getDate() - 7)
        const todayFormatted = today.toISOString().split('T')[0];
        const lastWeekFormatted = lastWeek.toISOString().split('T')[0];

        query.whereBetween('date', [lastWeekFormatted, todayFormatted])
      }
      const stringQuery = query.toString()
      const response = await this.connectionProvider.executeQuery(stringQuery)
      response.map(el => {
        el['actions'] = JSON.parse(el['actions'])
      })
      const filteredCampaigns = response.filter(campaign =>
        campaign.actions?.some(action =>
          action.action_type === "post_reaction" || action.action_type === "comment"
        )
      )
      const result = filteredCampaigns.map((campaign) => {
        const nomeInternoCampanha = campaign['Nome_Interno_Campanha']
        const postReactionObject = campaign?.actions.filter(action => action?.action_type === 'post_reaction')
        const likes = postReactionObject.length > 0 ? postReactionObject[0]['value'] : null
        const commentObject = campaign?.actions.filter(action => action?.action_type === 'comment')
        const comment = commentObject.length > 0 ? commentObject[0]['value'] : null
        const views = campaign['video_views']
        return {
          nomeInternoCampanha,
          likes,
          comment,
          views
        }
      })
      const aggregatedData = Object.values(this.aggregateCampaignData(result))

      return aggregatedData
    } catch (err) {
      console.error(err)
    }
  }

  async getPlatformMetrics(campaignName?: string, startDate?: string, endDate?: string) {
    try {
      let query = this.knexBuilder
        .select('platform')
        //.select('Nome_Interno_Campanha')
        .sum({ spend: 'metrics_cost' })
        .sum({ impressions: 'metrics_impressions' })
        .sum({ clicks: 'metrics_clicks' })
        .sum({ engagement: 'metrics_engagement' })
        .sum({ 'video_views_total': 'metrics_video_views' })
        .sum({ 'video_view_25': 'metrics_video_25p' })
        .sum({ 'video_view_50': 'metrics_video_50p' })
        .sum({ 'video_view_75': 'metrics_video_75p' })
        .sum({ 'video_view_100': 'metrics_video_100p' })
        .sum({ conversions_value: 'metrics_conversions_value' })
        .from('main.plataformas.plataformas_dia')
        .groupBy(['platform'])
        .orderBy('platform')

      if (campaignName) {
        query
          .select('Nome_Interno_Campanha')
          .groupBy('platform', 'Nome_Interno_Campanha')
          .where('Nome_Interno_Campanha', campaignName)
      }
      if (startDate && endDate) {
        query.whereBetween('date', [startDate, endDate])
      } else {
        const today = new Date()
        let lastWeek = new Date()
        lastWeek.setDate(today.getDate() - 7)
        const todayFormatted = today.toISOString().split('T')[0];
        const lastWeekFormatted = lastWeek.toISOString().split('T')[0];

        query.whereBetween('date', [lastWeekFormatted, todayFormatted])

      }
      const result = await this.connectionProvider.executeQuery(query.toString())

      const enrichedResult = result.map((row) => {
        const { spend, impressions, clicks, video_views_total, video_view_100 } = row;

        const safeSpend = spend || 0
        const safeImpressions = impressions || 0
        const safeClicks = clicks || 0
        const safeVideoViews = video_views_total || 0
        const safeMetricsVideo100 = video_view_100 || 0

        const CPM = safeImpressions > 0 ? (safeSpend / (safeImpressions / 1000)) : null
        const CPV = safeVideoViews > 0 ? (safeSpend / safeVideoViews) : null
        const CPC = safeClicks > 0 ? (safeSpend / safeClicks) : null
        const CTR = safeImpressions > 0 ? ((safeClicks / safeImpressions) * 100) : null
        const VTR = safeVideoViews > 0 ? (safeMetricsVideo100 / safeVideoViews) : null

        return {
          ...row,
          CPM,
          CPV,
          CPC,
          CTR,
          VTR
        };
      });

      return enrichedResult;
    } catch (err) {
      throw new HttpException(`Não foi possível buscar as informações, erro: ${err}`, HttpStatus.BAD_REQUEST)
    }

  }

  async getMetrics(campaignName?: string, startDate?: string, endDate?: string) {

    let baseQuery = this.knexBuilder
      .select('Nome_Interno_Campanha')
      .sum({ spend: 'metrics_cost' })
      .sum({ impressions: 'metrics_impressions' })
      .sum({ clicks: 'metrics_clicks' })
      .sum({ engagement: 'metrics_engagement' })
      .sum({ 'video_views_total': 'metrics_video_views' })
      .sum({ 'video_view_25%': 'metrics_video_25p' })
      .sum({ 'video_view_50%': 'metrics_video_50p' })
      .sum({ 'video_view_75%': 'metrics_video_75p' })
      .sum({ 'video_view_100%': 'metrics_video_100p' })
      .sum({ conversions_value: 'metrics_conversions_value' })
      .from('main.plataformas.plataformas_dia')
      .groupBy('Nome_Interno_Campanha')
      .orderBy('impressions', 'desc')

    if (!!startDate && !!endDate) {
      baseQuery = baseQuery
        .whereBetween('date', [startDate, endDate])
    } else {
      const today = new Date()
      let lastWeek = new Date()
      lastWeek.setDate(today.getDate() - 7)
      const todayFormatted = today.toISOString().split('T')[0];
      const lastWeekFormatted = lastWeek.toISOString().split('T')[0];

      baseQuery = baseQuery
        .whereBetween('date', [lastWeekFormatted, todayFormatted])
    }
    if (campaignName) {
      baseQuery.where('Nome_Interno_Campanha', campaignName)
    }

    const queryString = baseQuery.toString()
    console.log(queryString)
    console.log('\n')
    return this.connectionProvider.executeQuery(queryString)
  }
}
