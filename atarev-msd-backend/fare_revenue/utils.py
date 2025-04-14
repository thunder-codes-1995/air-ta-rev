from plotly.colors import n_colors

from base.constants import Constants


def class_mix(rbd_avgs, picked_year, picked_month):
    yearmo_colors = {}

    for cabin, cabin_df in rbd_avgs.groupby(["seg_class"]):
        curr = cabin_df.sort_values(by="blended_fare", ascending=False).reset_index(drop=True)
        if len(curr) > 1:
            colors = n_colors(
                Constants.CABIN_COLOR_MAP[cabin][0] if Constants.CABIN_COLOR_MAP.get(cabin) else "rgb(35, 144, 141)",
                Constants.CABIN_COLOR_MAP[cabin][1] if Constants.CABIN_COLOR_MAP.get(cabin) else "rgb(68, 211, 208)",
                curr.shape[0],
                colortype='rgb'
            )
        else:
            colors = [
                Constants.CABIN_COLOR_MAP[cabin][1] if Constants.CABIN_COLOR_MAP.get(cabin) else 'rgb(68, 211, 208)']
        yearmo_colors[(picked_year, picked_month, cabin)] = {(row["dom_op_al_code"], row["rbkd"]): colors[index] for
                                                             index, row in curr.iterrows()}

    return rbd_avgs, yearmo_colors
